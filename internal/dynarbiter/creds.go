// This file is the dynamo backend's answer to cmd/conch's readSecret: the
// one AWS credential pair the SDK client needs, and the one place that
// constructs the real *dynamodb.Client (client.go's DDB interface exists so
// the rest of the package never imports the SDK constructor at all). Kamal
// writes each accessory's secrets as a plain KEY=VALUE env file baked into
// the container at deploy time — no "export ", no quoting, no shell
// expansion — and LoadCreds trusts that shape rather than a general-purpose
// .env parser's.
package dynarbiter

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// LoadCreds parses a kamal-style env file (KEY=VALUE lines, #-comments,
// blank lines) and requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY. A
// line starting with "export " is refused with a pointed fault rather than
// silently accepted: kamal never writes that prefix into an accessory env
// file, so its presence means this is the wrong file, or a hand-edited copy
// of the right one — either way worth an operator's attention before conch
// authenticates to AWS with whatever it finds.
func LoadCreds(path string) (id, secret string, err error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", "", fmt.Errorf("reading arbiter.credentials_file %s: %w", path, err)
	}

	values := make(map[string]string)
	scanner := bufio.NewScanner(strings.NewReader(string(raw)))
	for lineNo := 1; scanner.Scan(); lineNo++ {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			return "", "", fmt.Errorf(
				"arbiter.credentials_file %s line %d: starts with \"export \", which kamal never writes to an "+
					"accessory env file — check this is the file kamal deployed and not a hand-edited copy",
				path, lineNo)
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			// Never echo the line itself: this is exactly the branch a
			// typo'd "KEY value" (space instead of "=") falls into, and the
			// value half of that typo is often the secret itself — naming
			// the line number is pointed enough without writing it to
			// stderr on a production host.
			return "", "", fmt.Errorf("arbiter.credentials_file %s line %d: not KEY=VALUE", path, lineNo)
		}
		values[strings.TrimSpace(key)] = strings.TrimSpace(val)
	}
	if err := scanner.Err(); err != nil {
		return "", "", fmt.Errorf("reading arbiter.credentials_file %s: %w", path, err)
	}

	idVal, idPresent := values["AWS_ACCESS_KEY_ID"]
	secretVal, secretPresent := values["AWS_SECRET_ACCESS_KEY"]

	var faults []string
	switch {
	case !idPresent:
		faults = append(faults, "missing AWS_ACCESS_KEY_ID")
	case idVal == "":
		faults = append(faults, "AWS_ACCESS_KEY_ID is empty")
	}
	switch {
	case !secretPresent:
		faults = append(faults, "missing AWS_SECRET_ACCESS_KEY")
	case secretVal == "":
		faults = append(faults, "AWS_SECRET_ACCESS_KEY is empty")
	}
	if len(faults) > 0 {
		return "", "", fmt.Errorf("arbiter.credentials_file %s: %s", path, strings.Join(faults, " and "))
	}
	return idVal, secretVal, nil
}

// NewDDB builds the real SDK client. endpoint "" = real AWS.
func NewDDB(region, endpoint, id, secret string) DDB {
	cfg := aws.Config{
		Region:      region,
		Credentials: credentials.NewStaticCredentialsProvider(id, secret, ""),
	}
	return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})
}
