Enron Scio
====

A simple data pipeline for answering questions about the Enron email dataset using [scio].

## How to run
The repo consists of a single file to run which answers 3 given questions. To run simply do:
```
sbt "runMain io.rgruener.emails.EmailAnalysis --emailsDirectory=<path to enron_with_categories directory>"
```

This will run on your local machine using the DirectRunner.

## Questions Answered

### Recipients
Which 3 recipients received the largest number of direct emails (emails that have exactly
one recipient), and how many did each receive?

- maureen.mcvicker@enron.com with 115 direct emails received.
- vkaminski@aol.com with 43 direct emails received.
- jeff.dasovich@enron.com with 25 direct emails received.


### Senders
Which 3 senders sent the largest number of broadcast emails 
(emails that have multiple recipients, including CCs and BCCs),
and how many did each send?

- steven.kean@enron.com with 251 broadcast emails sent.
- j.kaminski@enron.com with 40 broadcast emails sent.
- miyung.buster@enron.com with 31 broadcast emails sent.


### Response times
Find the 5 emails with the fastest response times. Please include file IDs,
subject, sender, recipient, and response time. (For our purposes, a response is defined as a
message from one of the recipients to the original sender whose subject line contains the subject
of the original email as a substring, and the response time should be measured as the difference
between when the original email was sent and when the response was sent.)

- lizzette.palmer@enron.com responded in 236 seconds by sending an email with the id
21343473.1075853118912.JavaMail.evans@thyme to the recipient michelle.cash@enron.com with
the subject,RE: CONFIDENTIAL Personnel issue at timestamp 1004113374. The original email had id
19096180.1075853121576.JavaMail.evans@thyme and was sent at 1004113138
- jeff.dasovich@enron.com responded in 240 seconds by sending an email with id
26937321.1075843427227.JavaMail.evans@thyme to karen.denne@enron.com with subject
Re: CONFIDENTIAL - Residential in CA at timestamp CA,987165240. The original email had id
15611890.1075843427202.JavaMail.evans@thyme and was sent at timestamp CA,987165000.
- jeff.dasovich@enron.com responded in 240 seconds by sending an email with id
2612882.1075843476998.JavaMail.evans@thyme to paul.kaufman@enron.com with the subject RE: Eeegads...
at timestamp 989502900. The original email had id 4889171.1075843476891.JavaMail.evans@thyme and 
was sent at the timestamp Eeegads...,989502660
- michelle.cash@enron.com responded in 322 seconds by sending an email with id
19096180.1075853121576.JavaMail.evans@thyme to lizzette.palmer@enron.com with the subject
RE: CONFIDENTIAL Personnel issue at timestamp 1004113138. The original email was sent with id
23471911.1075853121216.JavaMail.evans@thyme at timestamp 1004112816
- dana.davis@enron.com responded in 360 seconds by sending an email with id
15018793.1075853923235.JavaMail.evans@thyme to thane.twiggs@enron.com with the subject
Re: ISO-NE failure to mitigate ICAP market -- Release of ISO NE confidential information at the
timestamp 976716540. The original email was a broadcast email with id
20625717.1075857797770.JavaMail.evans@thyme send at timestamp 976716180.

## Answer Discussion

### Why Scio
I chose to use [scio] because as a contributor, I am familiar with the framework and was curious to
see how it would serve solving the use case of processing many different files (which is uncommon).
This exercise actually led to the addition of the [FileDownloadDoFn] which I copied into this repo
to avoid using a snapshot version. Prior to this, it was extremely difficult to treat many files
independently and perform some processing on each file before aggregating. Besides for fitting that
use case I did run into some serialization issues using scio and the Java Mail Parser but other than
that I am happy with my choice.

Given that scio allows for execution on Google Cloud Dataflow, spark, and other distributed processing
frameworks to scale this solution would simply involve uploading the files to GCS and changing the
arguments when running the job to execute it on Google Cloud Dataflow. (You may also need to include
the gcs nio dependency to list the files).

### Assumptions Made In This Solution
When performing this analysis, I needed to make some assumptions about the data to make things easier
to process and ultimately scale.

For starters, I made some basic assumptions on dealing with incorrectly formatted emails. Namely if
there are incorrectly formatted email addresses (known when my email parser throws an exception) they
are filtered out. This only applied to an extremely small percentage of emails so that was a safe
assumption which should not affect the end results. In addition although the Java Mail parser allows
for multiple senders, I assumed that an email can only have a single sender. Looking through the
dataset this applied to all emails so that was a safe assumption as well.

The largest assumptions were made when solving the response time portion of the problem. Given the
definition of a response, I would need to check almost all subjects of possible responses to check
for any substring (I actually show doing this in the code by reading all emails into memory which
would need to be distributed to all workers if using the Dataflow runner). Instead since I know that
the structure of responses is the original subject prepended by `re: ` (case insensitive) I can
check for responded by performing a very manageable join. Since I solved the response time problem
both ways I was able to verify that my assumption on response subjects was correct since the results
are identical.


[FileDownloadDoFn]: https://github.com/spotify/scio/blob/master/scio-extra/src/main/java/com/spotify/scio/extra/transforms/FileDownloadDoFn.java
[scio]: https://github.com/spotify/scio
