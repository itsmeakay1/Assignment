const express= require("express")
const app=express()
const bodyParser=require("body-parser")
let mysql = require('mysql2');
if (process.env.NODE_ENV !== "production") {
  require("dotenv").config();
}


let pool = mysql.createPool({
  connectionLimit: 10,
  host:process.env.NEWAWSENDPOINT,
  user: "admin",
  password:process.env.AWSPASSWORD,
  database:"bitespeeddb",
  port:"3306",
  connectTimeout: 15000, 

});


pool.getConnection(function(err, connection) {
  if (err) {
    console.error("Error connecting to MySQL:", err.message);
    process.exit(1); 
  }
  console.log("Connected!");
});
app.set('view engine', 'ejs');

app.use(express.json())
app.use(express.static('public'));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({
    extended: true
 }))
// To create a table
//  const tabledb = `
//  CREATE TABLE bitespeed (
//    id int NOT NULL AUTO_INCREMENT,
//    phoneNumber varchar(255) DEFAULT NULL,
//    email varchar(255) DEFAULT NULL,
//    linkedId int DEFAULT NULL,
//    linkPrecedence varchar(255) DEFAULT NULL,
//    createdAt varchar(255) DEFAULT NULL,
//    updatedAt varchar(255) DEFAULT NULL,
//    deletedAt varchar(255) DEFAULT NULL,
//    PRIMARY KEY (id)
//  )
// `;
// pool.query(tabledb, (err, result) => {
//   if (err) {
//     console.error('Error creating table:', err);
//   } else {
//     console.log('Table created successfully');
//   }
//   pool.end();
// });


app.get("/",(req,res)=>{
    res.render("index")
})

app.get("/identify",(req,res)=>{
  const getQuery='SELECT * FROM bitespeed'
  pool.query(getQuery,(err,result)=>{
    // console.log(result)
    res.send(result)
  })
})

app.post("/identify",(req,res)=>{
    // const {email,phoneNumber,createdAt,updatedAt}=req.body
    const {email,phoneNumber}=req.body
    const findEmailOrPhoneSql = `SELECT * FROM bitespeed WHERE email = "${email}" OR phoneNumber = "${phoneNumber}"`;
    const createdAt = new Date().toLocaleString("en-US", { timeZone: "Asia/Kolkata" });
    const updatedAt = new Date().toLocaleString("en-US", { timeZone: "Asia/Kolkata" });

    pool.query(findEmailOrPhoneSql, [email], (err, result) => {
      if (err) {
        // console.error('Error fetching data from MySQL:', err);
        return res.status(500).json({ error: 'Error fetching data from MySQL' });
        }
        if (result.length === 0) {
          //This block will insert the data because it doesnt exist
          const insertSql = `INSERT INTO bitespeed (email, phoneNumber,linkPrecedence, createdAt, updatedAt) VALUES ('${email}','${phoneNumber}',"primary" ,'${createdAt}','${updatedAt}')`;
          const insertValues=[email,phoneNumber,createdAt,updatedAt]
          pool.query(insertSql, insertValues, (err, insertedResult) => {
            if (err) {
              // console.error('Error inserting data into MySQL:', err);
              return res.status(500).json({ error: 'Error inserting data into MySQL' });
              }
              // This will print the newely entered data
              pool.query(findEmailOrPhoneSql, [email], (err, result) => {
                res.json(result)
              })
         })}else{
          //So it does exist
          const exisitingDataId=result[0]["id"]
          const insertExist = `INSERT INTO bitespeed (email, phoneNumber,linkedId,linkPrecedence, createdAt, updatedAt) VALUES ('${email}','${phoneNumber}','${exisitingDataId}','Secondary' ,'${createdAt}','${updatedAt}')`;
          const insertaddValues=[email,phoneNumber,exisitingDataId,"Secondary",createdAt,updatedAt]
          pool.query(insertExist, insertaddValues, (err, insertedResult) => {
          if (err) {
            // console.error('Error inserting data into MySQL:', err);
            return res.status(500).json({ error: 'Error inserting data into MySQL' });
            }
            pool.query(findEmailOrPhoneSql, [email], (err, result) => {
            res.json(result)

            })
            

        })
        }})
    });



// app.post("/identify",async(req,res)=>{
//     const {email,phoneNumber,createdAt,updatedAt}=req.body
    
//         const findSql = `SELECT * FROM bitespeed WHERE email ="${email}"`;
//         pool.query(findSql, [email], (err, result) => {
//             if (err) {
//             console.error('Error fetching data from MySQL:', err);
//             return res.status(500).json({ error: 'Error fetching data from MySQL' });
//           }
//           if (result.length === 0) {
//             //This block will insert the data because it doesnt exist
//             const insertSql = `INSERT INTO bitespeed (email, phoneNumber,linkPrecedence, createdAt, updatedAt) VALUES ('${email}','${phoneNumber}',"primary" ,'${createdAt}','${updatedAt}')`;
//             const insertValues=[email,phoneNumber,createdAt,updatedAt]
//             pool.query(insertSql, insertValues, (err, insertedResult) => {
//               if (err) {
//                 console.error('Error inserting data into MySQL:', err);
//                 return res.status(500).json({ error: 'Error inserting data into MySQL' });
//               }
//               // This will print the newely entered data
//               pool.query(findSql, [email], (err, result) => {
//                 res.json(result)
//               })
//         })}else{
//           res.json({result });
//         }
//         })
//     });



const PORT=process.env.PORT ||3000
app.listen(PORT,()=>{
    console.log(`Listening on port ${PORT}`)
})