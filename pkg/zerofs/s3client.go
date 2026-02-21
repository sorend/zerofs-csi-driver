package zerofs

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"k8s.io/klog/v2"
)

// S3Location holds the parsed components of an S3 storage URL.
type S3Location struct {
	Bucket string
	Prefix string
}

// parseS3URL parses an s3:// URL into bucket and key prefix.
// Example: "s3://my-bucket/zerofs-data/volumes/pvc-123" → {Bucket:"my-bucket", Prefix:"zerofs-data/volumes/pvc-123"}
func parseS3URL(rawURL string) (*S3Location, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid S3 URL %q: %w", rawURL, err)
	}
	if u.Scheme != "s3" {
		return nil, fmt.Errorf("URL scheme must be s3, got %q", u.Scheme)
	}
	bucket := u.Host
	if bucket == "" {
		return nil, fmt.Errorf("S3 URL %q has no bucket", rawURL)
	}
	prefix := strings.TrimPrefix(u.Path, "/")
	return &S3Location{Bucket: bucket, Prefix: prefix}, nil
}

// S3ClientConfig holds all the parameters needed to construct an S3 client.
type S3ClientConfig struct {
	AccessKeyID     string
	SecretAccessKey string
	Endpoint        string
	AllowHTTP       bool
	Region          string
}

// s3API is the subset of the S3 client interface used by this package.
// Defining an interface allows tests to inject a mock without a real S3 endpoint.
type s3API interface {
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
}

// newS3Client builds an aws-sdk-go-v2 S3 client from the supplied config.
func newS3Client(cfg S3ClientConfig) *s3.Client {
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}

	resolver := aws.EndpointResolverWithOptionsFunc(
		func(service, reg string, options ...interface{}) (aws.Endpoint, error) {
			if cfg.Endpoint != "" {
				scheme := "https"
				if cfg.AllowHTTP {
					scheme = "http"
				}
				ep := cfg.Endpoint
				if !strings.HasPrefix(ep, "http://") && !strings.HasPrefix(ep, "https://") {
					ep = scheme + "://" + ep
				}
				return aws.Endpoint{
					URL:               ep,
					HostnameImmutable: true,
					SigningRegion:     region,
				}, nil
			}
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		},
	)

	awsCfg := aws.Config{
		Region:                      region,
		EndpointResolverWithOptions: resolver,
	}

	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		awsCfg.Credentials = credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID, cfg.SecretAccessKey, "")
	}

	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
}

// deleteS3Prefix removes all objects under the given prefix (recursively) from
// the specified bucket using the provided S3 client.
//
// Objects are deleted in batches of up to 1000 (the S3 API limit).
// The function is idempotent: if no objects exist it returns without error.
func deleteS3Prefix(ctx context.Context, client s3API, bucket, prefix string) error {
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	klog.V(4).Infof("Deleting S3 objects under s3://%s/%s", bucket, prefix)

	var continuationToken *string
	totalDeleted := 0

	for {
		listOut, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return fmt.Errorf("failed to list S3 objects in s3://%s/%s: %w", bucket, prefix, err)
		}

		if len(listOut.Contents) == 0 {
			break
		}

		identifiers := make([]types.ObjectIdentifier, 0, len(listOut.Contents))
		for _, obj := range listOut.Contents {
			identifiers = append(identifiers, types.ObjectIdentifier{
				Key: obj.Key,
			})
		}

		delOut, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &types.Delete{
				Objects: identifiers,
				Quiet:   aws.Bool(true),
			},
		})
		if err != nil {
			return fmt.Errorf("failed to delete S3 objects in s3://%s/%s: %w", bucket, prefix, err)
		}
		if len(delOut.Errors) > 0 {
			// Report first error; in practice they are rare but should not be silenced.
			e := delOut.Errors[0]
			return fmt.Errorf("S3 delete error for key %s: %s", aws.ToString(e.Key), aws.ToString(e.Message))
		}

		totalDeleted += len(identifiers)
		klog.V(4).Infof("Deleted %d S3 objects (total so far: %d)", len(identifiers), totalDeleted)

		if listOut.IsTruncated == nil || !*listOut.IsTruncated {
			break
		}
		continuationToken = listOut.NextContinuationToken
	}

	klog.V(4).Infof("Successfully deleted %d S3 objects under s3://%s/%s", totalDeleted, bucket, prefix)
	return nil
}
