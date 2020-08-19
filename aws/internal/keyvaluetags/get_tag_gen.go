package keyvaluetags

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ec2"
)

// Ec2GetTag fetches an individual ec2 service tag for a resource.
// Returns whether the key exists, the key value, and any errors.
// This function will optimise the handling over Ec2ListTags, if possible.
// The identifier is typically the Amazon Resource Name (ARN), although
// it may also be a different identifier depending on the service.
func Ec2GetTag(conn *ec2.EC2, identifier string, key string) (bool, *string, error) {
	input := &ec2.DescribeTagsInput{
		Filters: []*ec2.Filter{
			{
				Name:   aws.String("resource-id"),
				Values: []*string{aws.String(identifier)},
			},
			{
				Name:   aws.String("key"),
				Values: []*string{aws.String(key)},
			},
		},
	}

	output, err := conn.DescribeTags(input)

	if err != nil {
		return false, nil, err
	}

	listTags := Ec2KeyValueTags(output.Tags)

	return listTags.KeyExists(key), listTags.KeyValue(key), nil
}
