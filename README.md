# sqs-file-spewer

This little program likes to eat text files. The only problem is they make him nauseous. You'll need to provide an SQS queue for him to throw up into. As a matter of fact, his spewing capabilities are what makes him so useful if you happen to need a bunch of JSON messages in SQS. 

# Quick start 

* Install rustup
* Run cargo install: 
```
cargo install --path . 
```
* Give him the filename, AWS Profile, Region and SQS Queue URL to use:
```
 sqs-file-spewer --filename tests/resources/test_messages.txt --profile TEST --region us-east-1 --q_url http://localhost:4567/queue/da-q
```

# Features

* Reads text files with SQS JSON messages on each line and spews them at a SQS queue. 

# Considerations

This tool makes no attempts to validate the JSON in the text file. As a matter of fact, it won't work with a JSON formatted file. It expects each **line** to be a proper JSON object, but it does not ensure this. This was an intentional design decision to allow for the quickest message processing which is the goal of the sqs-file-spewer. Each line is read from the text file and submitted to SQS as a JSON message with no validation.

# How fast does he spew?

He spews about 100 records per second during testing. This is running single core. This will be updated in further releases.

