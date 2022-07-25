aws cloudformation deploy --template-file C:/Users/dawid/team-1-project/templates/T1cloudformation.yaml --stack-name genteam1stk  --s3-bucket rawt3data --region eu-west-1 --parameter-overrides NotificationBucket=delon6-team1-raw-data DeploymentBucket=rawt3data DeploymentPackageKey=src.zip  --capabilities CAPABILITY_IAM

