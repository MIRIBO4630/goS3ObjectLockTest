package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func main() {

	bucket := flag.String("b", "", "The name of the bucket")
	filename := flag.String("f", "", "The file to upload")
	// parse the input arguments
	flag.Parse()

	// check the input arguments
	if *bucket == "" || *filename == "" {
		fmt.Println("You must supply a bucket name [-b BUCKET] and a filename [-f FILENAME]")
		return
	}

	// load the AWS configuration with the environment variables
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		fmt.Println("AWS configuration error, " + err.Error())
		return
	}
	// set your appropriate region
	cfg.Region = "us-east-1"

	// the service client for the next actions
	client := s3.NewFromConfig(cfg)

	// prepare the input for the new bucket with Object Locking
	inputCB := &s3.CreateBucketInput{
		Bucket:                     bucket,
		ObjectLockEnabledForBucket: true, // enable Object Locking for WORM / archiving purposes
	}

	// create the bucket with Object Lock
	_, err = client.CreateBucket(context.TODO(), inputCB)
	if err != nil {
		fmt.Printf("Could not create bucket %s \n" + *bucket)
		fmt.Println(err.Error())
	} else {
		fmt.Printf("%s bucket created!!! \n", *bucket)
	}

	// create the input for the default retention period - here: *** GOVERNANCE mode for 2 days ***
	inputPOLC := &s3.PutObjectLockConfigurationInput{
		Bucket: bucket,
		ObjectLockConfiguration: &types.ObjectLockConfiguration{ObjectLockEnabled: types.ObjectLockEnabledEnabled,
			Rule: &types.ObjectLockRule{DefaultRetention: &types.DefaultRetention{Mode: types.ObjectLockRetentionModeGovernance, Days: 2}}},
	}

	// put the default retention period on the bucket
	_, err = client.PutObjectLockConfiguration(context.TODO(), inputPOLC)
	if err != nil {
		fmt.Println("PutObjectLockConfiguration - error: ")
		fmt.Println(err.Error())
	} else {
		fmt.Println("PutObjectLockConfiguration - success!")
	}

	// now prepare the request for the Object Lock configuration
	inputGOLC := &s3.GetObjectLockConfigurationInput{
		Bucket: bucket,
	}

	// request the Object Lock settings
	out, err := client.GetObjectLockConfiguration(context.TODO(), inputGOLC)
	if err != nil {
		fmt.Println("GetObjectLockConfiguration - error: ")
		fmt.Println(err.Error())
	} else {
		// print the settings
		fmt.Println("ObjectLockEnabled:", out.ObjectLockConfiguration.ObjectLockEnabled)
		if out.ObjectLockConfiguration.Rule != nil {
			fmt.Println("DefaultRetention.Mode:", out.ObjectLockConfiguration.Rule.DefaultRetention.Mode)
			fmt.Println("DefaultRetention.Days:", out.ObjectLockConfiguration.Rule.DefaultRetention.Days)
		} else {
			fmt.Println(" but there is NO ObjectLockConfiguration.Rule <nil>")
		}
	}

	// prepare the upload of the file
	file, err := os.Open(*filename)
	if err != nil {
		fmt.Println("Unable to open file " + *filename)
		return
	}
	defer file.Close()

	// Get file size and read the file content into a buffer
	fileInfo, _ := file.Stat()
	var size int64 = fileInfo.Size()
	buffer := make([]byte, size)
	file.Read(buffer)

	// calculate a future date for the retention period of 1 day
	mtime := time.Now().UTC().Local()
	rt := mtime.AddDate(0, 0, 1)

	// determine the content type of your S3 object - file to be uploaded
	ct := http.DetectContentType(buffer)

	// create a md5hash to verify the content for the AWS file upload
	md5h := getMD5Hash(*filename)
	if md5h == "" {
		fmt.Println("no md5hash possible for:" + *filename)
		return
	}

	// upload the file into the bucket - an object with the appropriate parameters
	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:                    bucket,
		Key:                       filename,
		Body:                      bytes.NewReader(buffer),
		ContentLength:             size,
		ContentType:               &ct,
		ContentMD5:                &md5h,
		ObjectLockMode:            types.ObjectLockModeCompliance,
		ObjectLockRetainUntilDate: &rt,
	})
	if err != nil {
		fmt.Println("error:", err)
	} else {
		fmt.Printf("Putting of object %s into bucket %s has succeeded! \n", *filename, *bucket)
	}

	// prepare the request for existence of object in bucket
	inputHO := &s3.HeadObjectInput{
		Bucket: bucket,
		Key:    filename,
	}
	// perform the request for existence of object in bucket
	outHO, err := client.HeadObject(context.TODO(), inputHO)
	if err != nil {
		fmt.Printf("NO - object: %s in Bucket: %s does NOT exist! \n", *filename, *bucket)
	} else {
		fmt.Printf("YES - object: %s in Bucket: %s exists! \n", *filename, *bucket)
		fmt.Println("ObjectLockMode:", outHO.ObjectLockMode)
		fmt.Println("ObjectLockRetainUntilDate:", outHO.ObjectLockRetainUntilDate.Local())
	}

}

func getMD5Hash(filename string) (hash string) {

	// calculate the md5hash value for this file

	if filename == "" {
		return ""
	}
	file, err := os.Open(filename)
	if err != nil {
		return ""
	}
	defer file.Close()

	hasher := md5.New()
	_, err = io.Copy(hasher, file)
	if err != nil {
		log.Fatal(err)
		return ""
	}

	sum := hasher.Sum(nil)

	// the hash value must be base64 encoded to be accepted by AWS
	return (base64.StdEncoding.EncodeToString(sum))

}
