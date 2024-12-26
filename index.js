const express = require('express');
const path = require('path');
const fs = require('fs');
const mailSender = require('./mail-sender');
const app = express();
const port = 3000;

// Load email template
const emailTemplate = JSON.parse(fs.readFileSync('emailTemplate.json', 'utf8'));

// Define company names and emails
const companyNames = [
    "Third Unicorn",
    "Carvelle",
    "Psytech",
    "Juspay",
    "Flipkart",
    "Flipkart",
    "GetSwipe",
    "Hotstar",
    "Paytm",
    "Oyo",
    "Gameberrylabs",
    "Piramal",
    "Paypal",
    "Talkcharge",
    "Physics Wallah",
    "Zuraverse",
    "Datakalp",
    "Datakalp",
    "Threeway",
    "Listed Fans",
    "Listed Fans",
    "Listed Fans",
    "BrightMoney",
    "BankBazaar",
    "ControlF5",
    "Adbrew",
    "Adbrew",
];

const emails = [
    "rahul@third-unicorn.com",
    "careers@carvelle.in",
    "people@psytech.ai",
    "miskin.shravani@juspay.in",
    "abhinavsingh.k@flipkart.com",
    "anjali.verma@flipkart.com",
    "naveen+hiring@getswipe.in",
    "chaitrashree.hegde.con@hotstar.com",
    "hr@paytm.com",
    "shubhi.verma@oyorooms.com",
    "debasmita@gameberrylabs.com",
    "ravi.shankar@piramal.com",
    "r.chandrasit996@gmail.com",
    "careers@talkcharge.com",
    "prakriti.goel@pw.live",
    "hiring@zuraverse.xyz",
    "jayesh.jain@datakalp.com",
    "ta@datakalp.com",
    "contact@threeway.studio",
    "ad@listed.fans",
    "muskaan@listed.fans",
    "nandita@listed.fans",
    "mahalaxmi.desai@brightmoney.co",
    "careers@bankbazaar.com",
    "hr@controlf5.in",
    "hiring@adbrew.io",
    "hello@adbrew.io",
];

// API endpoint to send internship emails
app.get('/send-internship-emails', async (req, res) => {
    const name = 'Ayush Tyagi';
    const resumePath = path.join(__dirname, emailTemplate.resumeFileName); // Adjust path as needed

    // Ensure arrays are of the same length
    if (companyNames.length !== emails.length) {
        console.log('Error: The length of companyNames and emails arrays do not match.');
        return res.status(500).send('Internal Server Error: Data mismatch.');
    }

    for (let i = 0; i < companyNames.length; i++) {
        const companyName = companyNames[i];
        const email = emails[i];

        if (!email) {
            console.log(`Error: Missing email for company ${companyName} at index ${i}`);
            continue; // Skip this entry and continue with the next
        }

        const personalizedBody = emailTemplate.body
            .replace(/{name}/g, name)
            .replace(/{companyName}/g, companyName)
            .replace(/{receiverName}/g, "Sir/Madam");

        try {
            const info = await mailSender(email, emailTemplate.subject, personalizedBody, resumePath, emailTemplate.resumeFileName);
            console.log(`Email sent to ${email}: ${info.response}`);
        } catch (error) {
            console.log(`Error sending email to ${email}: ${error.message}`);
        }
    }

    res.send('Internship application emails scheduled successfully!');
});

app.listen(port, () => {
    console.log(`Server is running on http://localhost:${port}`);
});
