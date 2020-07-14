package aws

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/hashicorp/terraform-plugin-sdk/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/terraform"
)

func init() {
	resource.AddTestSweepers("aws_rds_cluster_activity_stream", &resource.Sweeper{
		Name: "aws_rds_cluster_activity_stream",
		F:    func(region string) error { return nil },
		Dependencies: []string{
			"aws_kms_key",
			"aws_kinesis_stream",
			"aws_rds_cluster",
		},
	})
}

func TestAccAWSRDSClusterActivityStream(t *testing.T) {
	var dbCluster rds.DBCluster
	rName := acctest.RandString(5)
	resourceName := "aws_rds_cluster_activity_stream.default"

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAWSClusterActivityStreamDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAWSClusterActivityStreamConfig(rName),
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

		if aws.StringValue(v.ActivityStreamStatus) != rds.ActivityStreamStatusStarted {
			return fmt.Errorf("incorrect activity stream status: expected: %s, got: %s", rds.ActivityStreamStatusStarted, aws.StringValue(v.ActivityStreamStatus))
		}

		if aws.StringValue(v.ActivityStreamMode) != "sync" && aws.StringValue(v.ActivityStreamMode) != "async" {
			return fmt.Errorf("incorrect activity stream mode: expected: sync or async, got: %s", aws.StringValue(v.ActivityStreamMode))
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
				*resp.DBClusters[0].ActivityStreamStatus != rds.ActivityStreamStatusStopped {
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

func testAccAWSClusterActivityStreamConfig(rName string) string {
	return fmt.Sprintf(`
data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_kms_key" "default" {
	description             = "tf-testacc-kms-key-%[1]s"
  deletion_window_in_days = 7
}

resource "aws_rds_cluster" "default" {
  cluster_identifier              = "tf-testacc-aurora-cluster-%[1]s"
  engine                  				= "aurora-postgresql"
  engine_version                  = "10.11"
  availability_zones              = ["${data.aws_availability_zones.available.names[0]}"]
  database_name                   = "mydb"
  master_username                 = "foo"
  master_password                 = "mustbeeightcharaters"
  db_cluster_parameter_group_name = "default.aurora-postgresql10"
  skip_final_snapshot             = true
  deletion_protection             = false
}

resource "aws_rds_cluster_instance" "default" {
	identifier         = "tf-testacc-aurora-instance-%[1]s"
  cluster_identifier = "${aws_rds_cluster.default.cluster_identifier}"
  engine             = "${aws_rds_cluster.default.engine}"
  instance_class     = "db.r5.large"
}

resource "aws_rds_cluster_activity_stream" "default" {
  arn  								= "${aws_rds_cluster.default.arn}"
  apply_immediately  	= true
  kms_key_id 					= "${aws_kms_key.default.arn}"
  mode         				= "async"
}
`, rName)
}
