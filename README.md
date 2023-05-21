# For correct work you should Download file from AWS: s3://amazon-reviews-pds/tsv/amazon_reviews_us_Camera_v1_00.tsv.gz, extract it and put to resource folder. 

Result of work: 
The data did not pass the quality checks, the reasons are:
ComplianceConstraint(Compliance(review_date is a valid date,TO_DATE(review_date, 'yyyy-MM-dd') IS NOT NULL,None)): Value: 0.9999678130761043 does not meet the constraint requirement!

