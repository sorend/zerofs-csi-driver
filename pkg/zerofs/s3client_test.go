package zerofs

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// --------------------------------------------------------------------------
// Mock S3 client
// --------------------------------------------------------------------------

type mockS3Client struct {
	// listPages is a slice of pages to return in sequence.
	listPages [][]types.Object
	listErr   error
	listCalls int

	deleteRequests []*s3.DeleteObjectsInput
	deleteErr      error
	deleteErrKeys  []string // keys to return as DeleteObjects errors
}

func (m *mockS3Client) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	idx := m.listCalls
	m.listCalls++
	if idx >= len(m.listPages) {
		return &s3.ListObjectsV2Output{IsTruncated: aws.Bool(false)}, nil
	}
	truncated := idx < len(m.listPages)-1
	return &s3.ListObjectsV2Output{
		Contents:    m.listPages[idx],
		IsTruncated: aws.Bool(truncated),
		NextContinuationToken: func() *string {
			if truncated {
				t := fmt.Sprintf("token-%d", idx+1)
				return &t
			}
			return nil
		}(),
	}, nil
}

func (m *mockS3Client) DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	if m.deleteErr != nil {
		return nil, m.deleteErr
	}
	m.deleteRequests = append(m.deleteRequests, params)

	var errs []types.Error
	for _, key := range m.deleteErrKeys {
		k := key
		errs = append(errs, types.Error{
			Key:     &k,
			Message: aws.String("AccessDenied"),
		})
	}
	return &s3.DeleteObjectsOutput{Errors: errs}, nil
}

func makeObjects(keys ...string) []types.Object {
	objs := make([]types.Object, len(keys))
	for i, k := range keys {
		key := k
		objs[i] = types.Object{Key: &key}
	}
	return objs
}

// --------------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------------

var _ = ginkgo.Describe("parseS3URL", func() {
	ginkgo.It("should parse a simple s3 URL", func() {
		loc, err := parseS3URL("s3://my-bucket/path/to/prefix")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(loc.Bucket).To(gomega.Equal("my-bucket"))
		gomega.Expect(loc.Prefix).To(gomega.Equal("path/to/prefix"))
	})

	ginkgo.It("should parse an s3 URL with no path", func() {
		loc, err := parseS3URL("s3://my-bucket")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(loc.Bucket).To(gomega.Equal("my-bucket"))
		gomega.Expect(loc.Prefix).To(gomega.Equal(""))
	})

	ginkgo.It("should parse an s3 URL with trailing slash", func() {
		loc, err := parseS3URL("s3://my-bucket/volumes/pvc-abc/")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(loc.Bucket).To(gomega.Equal("my-bucket"))
		gomega.Expect(loc.Prefix).To(gomega.Equal("volumes/pvc-abc/"))
	})

	ginkgo.It("should return error for non-s3 scheme", func() {
		_, err := parseS3URL("https://my-bucket/path")
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("scheme must be s3"))
	})

	ginkgo.It("should return error for missing bucket", func() {
		_, err := parseS3URL("s3:///path")
		gomega.Expect(err).To(gomega.HaveOccurred())
	})

	ginkgo.It("should return error for invalid URL", func() {
		_, err := parseS3URL("://bad")
		gomega.Expect(err).To(gomega.HaveOccurred())
	})
})

var _ = ginkgo.Describe("deleteS3Prefix", func() {
	ginkgo.It("should do nothing when bucket is empty", func() {
		mock := &mockS3Client{listPages: [][]types.Object{}}
		err := deleteS3Prefix(context.Background(), mock, "my-bucket", "volumes/pvc-abc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(mock.deleteRequests).To(gomega.BeEmpty())
	})

	ginkgo.It("should delete all objects in a single page", func() {
		mock := &mockS3Client{
			listPages: [][]types.Object{
				makeObjects("volumes/pvc-abc/a", "volumes/pvc-abc/b", "volumes/pvc-abc/c"),
			},
		}
		err := deleteS3Prefix(context.Background(), mock, "my-bucket", "volumes/pvc-abc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(mock.deleteRequests).To(gomega.HaveLen(1))
		gomega.Expect(mock.deleteRequests[0].Delete.Objects).To(gomega.HaveLen(3))
	})

	ginkgo.It("should delete objects across multiple pages", func() {
		mock := &mockS3Client{
			listPages: [][]types.Object{
				makeObjects("volumes/pvc-abc/1", "volumes/pvc-abc/2"),
				makeObjects("volumes/pvc-abc/3", "volumes/pvc-abc/4"),
			},
		}
		err := deleteS3Prefix(context.Background(), mock, "my-bucket", "volumes/pvc-abc")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(mock.deleteRequests).To(gomega.HaveLen(2))
		// The mock sets IsTruncated=false on the last page, so exactly 2 list calls are made.
		gomega.Expect(mock.listCalls).To(gomega.Equal(2))
	})

	ginkgo.It("should return error when ListObjectsV2 fails", func() {
		mock := &mockS3Client{listErr: fmt.Errorf("access denied")}
		err := deleteS3Prefix(context.Background(), mock, "my-bucket", "volumes/pvc-abc")
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("list S3 objects"))
	})

	ginkgo.It("should return error when DeleteObjects API call fails", func() {
		mock := &mockS3Client{
			listPages: [][]types.Object{makeObjects("volumes/pvc-abc/x")},
			deleteErr: fmt.Errorf("network error"),
		}
		err := deleteS3Prefix(context.Background(), mock, "my-bucket", "volumes/pvc-abc")
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("delete S3 objects"))
	})

	ginkgo.It("should return error when DeleteObjects returns per-key errors", func() {
		mock := &mockS3Client{
			listPages:     [][]types.Object{makeObjects("volumes/pvc-abc/locked")},
			deleteErrKeys: []string{"volumes/pvc-abc/locked"},
		}
		err := deleteS3Prefix(context.Background(), mock, "my-bucket", "volumes/pvc-abc")
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("S3 delete error"))
	})

	ginkgo.It("should append a trailing slash to prefix if missing", func() {
		var capturedPrefix string
		mock := &mockS3ClientCapture{onList: func(prefix string) { capturedPrefix = prefix }}
		_ = deleteS3Prefix(context.Background(), mock, "my-bucket", "volumes/pvc-abc")
		gomega.Expect(capturedPrefix).To(gomega.Equal("volumes/pvc-abc/"))
	})
})

// mockS3ClientCapture lets tests inspect what prefix was actually passed to ListObjectsV2.
type mockS3ClientCapture struct {
	onList func(prefix string)
}

func (m *mockS3ClientCapture) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if m.onList != nil {
		m.onList(aws.ToString(params.Prefix))
	}
	return &s3.ListObjectsV2Output{IsTruncated: aws.Bool(false)}, nil
}

func (m *mockS3ClientCapture) DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	return &s3.DeleteObjectsOutput{}, nil
}
