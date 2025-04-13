const nodemailer = require('nodemailer');
const fs = require('fs');
const path = require('path');
require('dotenv').config();

const mailSender = async (email, title, body, attachmentPath = null, attachmentName = null) => {
    try {
        // Check if recipient email is provided
        if (!email) {
            throw new Error('Recipient email is required.');
        }

        let transporter = nodemailer.createTransport({
            host: process.env.MAIL_HOST,
            port: process.env.MAIL_PORT,
            secure: false, // true for 465, false for other ports
            auth: {
                user: process.env.MAIL_USER,
                pass: process.env.MAIL_PASS,
            },
        });

        // Prepare mail options
        let mailOptions = {
            from: `"Ankit" <${process.env.MAIL_USER}>`,
            to: email,
            subject: title,
            html: body,
        };

        // Add attachment if provided
        if (attachmentPath && attachmentName) {
            const attachment = {
                filename: attachmentName,
                path: attachmentPath,
            };
            mailOptions.attachments = [attachment];
        }

        // Send mail
        let info = await transporter.sendMail(mailOptions);

        return info;
    } catch (e) {
        console.log(`Error: ${e.message}`);
        throw e; // Re-throw the error to handle it in the caller function
    }
};

module.exports = mailSender;
