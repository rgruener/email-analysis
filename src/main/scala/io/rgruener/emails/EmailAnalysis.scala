package io.rgruener.emails

import java.io.{ByteArrayInputStream, File}
import java.net.URI
import java.nio.file.Path
import java.util.Properties
import javax.mail.Session
import javax.mail.internet.MimeMessage

import com.spotify.scio.ContextAndArgs
import org.apache.beam.sdk.transforms.{ParDo, SerializableFunction}

import scala.util.Try

//noinspection ScalaStyle
object EmailAnalysis {

  case class EmailMessage(emailId: String,
                          sender: String,
                          recipients: Set[String],
                          subject: String,
                          sentTs: Long)

  private def mimeMessageToCaseClass(mimeMessage: MimeMessage): Option[EmailMessage] = {
    // Filter out emails with invalid emails addresses.
    Try {
      val emailId = mimeMessage.getMessageID
      val recipients = Try(mimeMessage.getAllRecipients.map(_.toString)).getOrElse(Array())

      // From always has exactly one element in the array according to data analysis
      val sender = mimeMessage.getFrom.head.toString
      val subject = mimeMessage.getSubject
      val sentTime = mimeMessage.getSentDate.toInstant.getEpochSecond

      EmailMessage(emailId, sender, recipients.toSet, subject, sentTime)
    }.toOption
  }

  private def validResponse(original: EmailMessage, response: EmailMessage): Boolean = {
    // A valid response has a subject with the original subject as a substring
    response.subject.contains(original.subject) &&
    // A valid response must be sent after the original message
      response.sentTs > original.sentTs &&
    // A valid response must not be a person responding to their own email
      !original.sender.equals(response.sender) &&
    // A valid response must have the sender be in original recipients
      original.recipients.contains(response.sender) &&
    // A valid response must of the original sender in the recipients
      response.recipients.contains(original.sender)
  }

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    @transient lazy val session = Session.getDefaultInstance(new Properties())


    // Get list of all email txt files
    // This utilizes knowing about the file structure
    val emailsDir = args("emailsDirectory")
    val emailPaths = (1 until 9).flatMap { i =>
      new File(s"$emailsDir/$i").listFiles()
        .filter(file => file.isFile && file.getPath.endsWith(".txt"))
        .map(file => new URI(file.getAbsolutePath))
    }

    val emails = sc.parallelize(emailPaths)
      .applyTransform(ParDo.of(new FileDownloadFn[String](new SerializableFunction[Path, String] {
        override def apply(input: Path): String = scala.io.Source.fromFile(input.toFile).getLines().mkString("\n")
      }, 4096, false)))
      .flatMap { emailText =>
        val is = new ByteArrayInputStream(emailText.getBytes)
        val message = new MimeMessage(session, is)
        is.close()
        // Convert the mime message object to a case class due to serialization issues
        mimeMessageToCaseClass(message)
      }

    // Which 3 recipients received the largest number of direct emails (emails that have
    // exactly one recipient), and how many did each receive?
    val topNDirectRecipients = args.int("topNDirectRecipients", 3)
    val topDirectTap = emails
      // Filter out only direct emails (with only 1 total recipient
      .filter(email => email.recipients.size == 1)
      // Map out just the single recipient
      .map(email => email.recipients.head.toString)
      // Count the total number of emails received by each direct recipient
      .countByValue
      // Take the top N direct recipients
      .top(topNDirectRecipients)(Ordering.by { case (_, directEmailCount) => directEmailCount})
      // Materialize to local machine to print out results
      .materialize

    // Which 3 senders sent the largest number of broadcast emails (emails that have multiple
    // recipients, including CCs and BCCs), and how many did each send?
    val topNBroadcastSenders = args.int("topNBroadcastSenders", 3)
    val topBroadcastersTap = emails
      // Filter out only broadcaste emails (more than 1 recipient)
      .filter(email => email.recipients.size > 1)
      // Map out the senders of said emails (removing empty options)
      .map(email => email.sender)
      // Count the number of broadcasts for each sender
      .countByValue
      // Take the top N broadcast senders
      .top(topNBroadcastSenders)(Ordering.by { case (_, broadcastSendCount) => broadcastSendCount})
      // Materialize to local machine to print out results
      .materialize

    // Find the 5 emails with the fastest response times.
    val topNFastestResponses = args.int("topNFastestResponses", 5)

    // First solution is a bad N^2 one where we read all the emails into memory on the workers
    // This will not work on larger datasets but can be used to validate some assumptions.

    // Make side input of all emails
    val sideIn = emails.asIterableSideInput
    val topResponseTimesTapNotScaleable = emails
      .withSideInputs(sideIn)
      // For every email, find the valid responses using the side input
      .flatMap { case (email, ctx) =>
        ctx(sideIn)
          .filter(response => validResponse(email, response))
          .map(response => (email, response))
      }
      .toSCollection
      // Calculate the response times
      .map { case (original, response) =>
        (original, response, response.sentTs - original.sentTs)
      }
      // Find fastest responses globally
      .top(topNFastestResponses)(Ordering.by { case (_, _, responseTime) => -responseTime})
      // Materialize to local machine to print out results
      .materialize

    // Now lets try by joining on a composite key of subject and email sender/recipient pairs
    val responses = emails.map { email =>
      // Since we expect all responses to contain the original emails subject with an re at
      // the beginning. We can simply use the subject with an re: prepended to match on responses.
      // I convert to lower case to handle cases such as RE: Re: and re:
      val respSubject = if (email.subject.toLowerCase.startsWith("re: ")) {
        email.subject.toLowerCase
      } else {
        s"re: ${email.subject.toLowerCase}"
      }
      ((respSubject, email.sender), email)
    }

    val topResponseTimesTap = emails
      .flatMap { email =>
        // Do the same thing for originally sent emails
        val respSubject = if (email.subject.toLowerCase.startsWith("re: ")) {
          email.subject.toLowerCase
        } else {
          s"re: ${email.subject.toLowerCase}"
        }
        email.recipients.map(recipient => ((respSubject, recipient), email))
      }
      // Join possible responses with possible original emails
      .join(responses)
      // Map out the original and response
      .values
      // Filter out valid responses (from the already extremely filtered set)
      .filter { case (original, response) => validResponse(original, response) }
      // Calculate the response times
      .map { case (original, response) =>
        (original, response, response.sentTs - original.sentTs)
      }
      // Retrieve the top N fastest response times
      .top(topNFastestResponses)(Ordering.by { case (_, _, responseTime) => -responseTime})
      // Materialize to local machine
      .materialize



    // Need to block until done in order to be able to call get below on the future
    sc.close().waitUntilDone()

    println(s"\n---------------------------- ANSWERS ----------------------------\n")

    val topDirectRecipients = topDirectTap.value.get.get.value.flatten
    println(s"The Top $topNDirectRecipients Recipients Of Direct Email Are:")
    topDirectRecipients.foreach { case (recipient, directEmailCount) =>
      println(s"$recipient with $directEmailCount direct emails received.")
    }

    val topBroadcasters = topBroadcastersTap.value.get.get.value.flatten
    println(s"\nThe Top $topNBroadcastSenders Senders Of Broadcast Email Are:")
    topBroadcasters.foreach { case (sender, broadcastCount) =>
      println(s"$sender with $broadcastCount broadcast emails sent.")
    }

    val topResponseTimes = topResponseTimesTap.value.get.get.value.flatten
    println(s"\nThe Top $topNFastestResponses Responses Are:")
    topResponseTimes.foreach { case (email, response, responseTime) =>
      println(s"$responseTime seconds \nFrom $response \nTo $email\n")
    }

    // Finally assert that the top response times calculated with my subject assumption is the
    // same as the bad N^2 solution
    val topResponseTimesN2 = topResponseTimesTapNotScaleable.value.get.get.value.flatten
    assert(topResponseTimes.zip(topResponseTimesN2)
      .forall { case ((email1, response1, _), (email2, response2, _)) =>
        email1.equals(email2) && response1.equals(response2)
      }
    )

  }
}
