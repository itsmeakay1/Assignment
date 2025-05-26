const express = require("express");
const app = express();
const bodyParser = require("body-parser");
const mysql = require("mysql2");

if (process.env.NODE_ENV !== "production") {
  require("dotenv").config();
}

let pool = mysql.createPool({
  connectionLimit: 10,
  host: process.env.NEWAWSENDPOINT,
  user: "admin",
  password: process.env.AWSPASSWORD,
  database: "bitespeeddb",
  port: "3306",
  connectTimeout: 15000,
});

pool.getConnection(function (err, connection) {
  if (err) {
    console.error("Error connecting to MySQL:", err.message);
    process.exit(1);
  }
  console.log("Connected to MySQL!");
});

app.set("view engine", "ejs");

app.use(express.json());
app.use(express.static("public"));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

app.get("/", (req, res) => {
  res.render("index");
});

app.get("/identify", (req, res) => {
  const getQuery = "SELECT * FROM bitespeed";
  pool.query(getQuery, (err, result) => {
    res.send(result);
  });
});

app.post("/identify", (req, res) => {
  const { email, phoneNumber } = req.body;
  const createdAt = new Date().toISOString();
  const updatedAt = new Date().toISOString();

  if (!email && !phoneNumber) {
    return res.status(400).json({ error: "Email or phoneNumber required" });
  }

  const findAllMatchingContacts = `
    SELECT * FROM bitespeed
    WHERE email = ? OR phoneNumber = ? OR linkedId IN (
      SELECT id FROM (SELECT id FROM bitespeed WHERE email = ? OR phoneNumber = ?) AS sub
    )
  `;

  pool.query(findAllMatchingContacts, [email, phoneNumber, email, phoneNumber], (err, contacts) => {
    if (err) return res.status(500).json({ error: "Database error" });

    // No matches: create new primary contact
    if (contacts.length === 0) {
      const insertNew = `
        INSERT INTO bitespeed (email, phoneNumber, linkPrecedence, createdAt, updatedAt)
        VALUES (?, ?, 'primary', ?, ?)
      `;
      pool.query(insertNew, [email, phoneNumber, createdAt, updatedAt], (err, result) => {
        if (err) return res.status(500).json({ error: "Insert failed" });

        return res.status(200).json({
          contact: {
            primaryContactId: result.insertId,
            emails: [email],
            phoneNumbers: [phoneNumber],
            secondaryContactIds: [],
          },
        });
      });
    } else {
      // Match found: determine primary
      const allContacts = [...contacts];
      const primaryContact = allContacts.reduce((oldest, curr) => {
        return new Date(curr.createdAt) < new Date(oldest.createdAt) ? curr : oldest;
      });
      const primaryId = primaryContact.linkPrecedence === 'primary' ? primaryContact.id : primaryContact.linkedId;

      const knownEmails = new Set(allContacts.map(c => c.email).filter(Boolean));
      const knownPhones = new Set(allContacts.map(c => c.phoneNumber).filter(Boolean));

      const isNewInfo =
        (email && !knownEmails.has(email)) ||
        (phoneNumber && !knownPhones.has(phoneNumber));

      const maybeInsertNewSecondary = () => {
        if (!isNewInfo) return finalizeResponse();

        const insertSecondary = `
          INSERT INTO bitespeed (email, phoneNumber, linkedId, linkPrecedence, createdAt, updatedAt)
          VALUES (?, ?, ?, 'secondary', ?, ?)
        `;
        pool.query(insertSecondary, [email, phoneNumber, primaryId, createdAt, updatedAt], (err) => {
          if (err) return res.status(500).json({ error: "Failed to insert secondary" });
          finalizeResponse();
        });
      };

      const finalizeResponse = () => {
        const fetchAllRelated = `
          SELECT * FROM bitespeed
          WHERE id = ? OR linkedId = ? OR linkedId IN (
            SELECT id FROM bitespeed WHERE linkedId = ?
          )
        `;
        pool.query(fetchAllRelated, [primaryId, primaryId, primaryId], (err, fullContacts) => {
          if (err) return res.status(500).json({ error: "Fetch error" });

          const emails = [...new Set(fullContacts.map(c => c.email).filter(Boolean))];
          const phones = [...new Set(fullContacts.map(c => c.phoneNumber).filter(Boolean))];
          const secondaryIds = fullContacts
            .filter(c => c.linkPrecedence === "secondary")
            .map(c => c.id);

          return res.status(200).json({
            contact: {
              primaryContactId: primaryId,
              emails,
              phoneNumbers: phones,
              secondaryContactIds: secondaryIds,
            },
          });
        });
      };

      maybeInsertNewSecondary();
    }
  });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Listening on port ${PORT}`);
});
