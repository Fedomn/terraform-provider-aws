package aws

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/hashicorp/terraform-plugin-sdk/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/terraform"
)

func testAccAWSClusterActivityStreamConfig() string {
	return fmt.Sprintf(`
resource "aws_rds_cluster_activity_stream" "default" {
  arn  								= "database-1"
  apply_immediately  	= true
  kms_key_id 					= "db-kms-id"
  mode         				= "async"
}
`)
}

func TestAccAWSRDSClusterActivityStream(t *testing.T) {
	var dbCluster rds.DBCluster
	resourceName := "aws_rds_cluster_activity_stream.default"

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAWSClusterActivityStreamDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAWSClusterActivityStreamConfig(),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSRDSClusterActivityStreamExists(resourceName, &dbCluster),
					testAccCheckAWSRDSClusterActivityStreamAttributes(&dbCluster),
					resource.TestCheckResourceAttrSet(resourceName, "arn"),
					resource.TestCheckResourceAttrSet(resourceName, "kms_key_id"),
					resource.TestCheckResourceAttrSet(resourceName, "kinesis_stream_name"),
					resource.TestCheckResourceAttr(resourceName, "mode", "started"),
				),
			},
		},
	})
}

func testAccCheckAWSRDSClusterActivityStreamExists(resourceName string, dbCluster *rds.DBCluster) resource.TestCheckFunc {
	return testAccCheckAWSRDSClusterActivityStreamExistsWithProvider(resourceName, dbCluster, testAccProvider)
}

func testAccCheckAWSRDSClusterActivityStreamExistsWithProvider(resourceName string, dbCluster *rds.DBCluster, provider *schema.Provider) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[resourceName]
		if !ok {
			return fmt.Errorf("Not found: %s", resourceName)
		}

		if rs.Primary.ID == "" {
			return fmt.Errorf("DBCluster ID is not set")
		}

		conn := provider.Meta().(*AWSClient).rdsconn

		response, err := conn.DescribeDBClusters(&rds.DescribeDBClustersInput{
			DBClusterIdentifier: aws.String(rs.Primary.ID),
		})

		if err != nil {
			return err
		}

		if len(response.DBClusters) != 1 ||
			*response.DBClusters[0].DBClusterArn != rs.Primary.ID {
			return fmt.Errorf("DBCluster not found")
		}

		*dbCluster = *response.DBClusters[0]
		return nil
	}
}

func testAccCheckAWSRDSClusterActivityStreamAttributes(v *rds.DBCluster) resource.TestCheckFunc {
	return func(s *terraform.State) error {

		if aws.StringValue(v.DBClusterArn) == "" {
			return fmt.Errorf("empty RDS Cluster arn")
		}

		if aws.StringValue(v.ActivityStreamKmsKeyId) == "" {
			return fmt.Errorf("empty RDS Cluster activity stream kms key id")
		}
		if aws.StringValue(v.ActivityStreamKinesisStreamName) == "" {
			return fmt.Errorf("empty RDS Cluster activity stream kinesis stream name")
		}

		if aws.StringValue(v.ActivityStreamStatus) != "sync" &&
			aws.StringValue(v.ActivityStreamStatus) != "async" {
			return fmt.Errorf("Incorrect activity stream status: expected: sync or async, got: %s", aws.StringValue(v.ActivityStreamStatus))
		}

		return nil
	}
}

func testAccCheckAWSClusterActivityStreamDestroy(s *terraform.State) error {
	return testAccCheckAWSClusterActivityStreamDestroyWithProvider(s, testAccProvider)
}

func testAccCheckAWSClusterActivityStreamDestroyWithProvider(s *terraform.State, provider *schema.Provider) error {
	conn := provider.Meta().(*AWSClient).rdsconn

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "aws_rds_cluster_activity_stream" {
			continue
		}

		// Try to find the Group
		var err error
		resp, err := conn.DescribeDBClusters(
			&rds.DescribeDBClustersInput{
				DBClusterIdentifier: aws.String(rs.Primary.ID),
			})

		if err == nil {
			if len(resp.DBClusters) != 0 &&
				*resp.DBClusters[0].ActivityStreamStatus != rds.ActivityStreamStatusStopping {
				return fmt.Errorf("DB Cluster %s Activity Stream still exists", rs.Primary.ID)
			}
		}

		// Return nil if the cluster is already destroyed
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "DBClusterNotFoundFault" {
				return nil
			}
		}

		return err
	}

	return nil
}
