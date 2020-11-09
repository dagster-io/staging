aws_credentials=$(aws sts assume-role --role-arn arn:aws:iam::007292508084:role/TestMapboxIAMRole --role-session-name "test_session")

export AWS_ACCESS_KEY_ID=$(echo $aws_credentials|jq '.Credentials.AccessKeyId'|tr -d '"')
export AWS_SECRET_ACCESS_KEY=$(echo $aws_credentials|jq '.Credentials.SecretAccessKey'|tr -d '"')
export AWS_SESSION_TOKEN=$(echo $aws_credentials|jq '.Credentials.SessionToken'|tr -d '"')

aws sts decode-authorization-message --encoded-message $1 | jq .DecodedMessage -r | jq