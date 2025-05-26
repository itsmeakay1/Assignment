<h1>Bitespeed Backend Task: Identity Reconciliation</h1>
<hr>
<h2>Overview</h2>
<p>This is backend Node.js application built using Express framework and AWS RDS (Amazon Relational Database Service) as the MySQL server. The purpose of this project is to perform identity reconciliation, where incoming data in JSON format is received, processed, and stored in the SQL Database. Additionally, the application allows for retrieving data associated with a specific email address.</p>

<h2>Function</h2>
<ul>
<li>Receive incoming data in JSON format and perform identity reconciliation.</li>
<li>Store identity data in the AWS RDS MySQL database.</li>
<li>Provide an endpoint to retrieve identity data associated with a specific email address.</li>
</ul>

<h3>SQL Table</h3>

The <code>bitespeed</code> table in the AWS RDS MySQL database is used to store identity data received from incoming JSON requests. It contains the following columns:
<ol>
<li>id (Primary Key): Auto-incrementing integer Column</li>
<li>email: A VARCHAR(255) Column</li>
<li>phoneNumber: A VARCHAR(20) Column</li>
<li>linkedId: An integer Column</li>
<li>linkPrecedence: A VARCHAR(255) Column</li>
<li>createdAt: A VARCHAR(255) Column</li>
<li>updatedAt: A VARCHAR(255) Column</li>
<li>deletedAt: A VARCHAR(255) Column </li>
</ol>
<hr>
<h6>Input</h6>
```
Email: 1@gmail.com
PhoneNumber:123456
```

<h6>Output</h6>
```
[{"id":4,"phoneNumber":"123456","email":"1@gmail.com","linkedId":null,"linkPrecedence":"primary","createdAt":"7/24/2023, 9:49:01 PM","updatedAt":"7/24/2023, 9:49:01 PM","deletedAt":null}]
```
<hr>
<p>Please find below the task link</p>
<a href="https://bitespeed-6pje.onrender.com/">Task Link</a>

<h3> Challenges Faced</h3>
<p>My goal for attempting this task was to learn more about integrating MySql with Node, as I had prior experience with Node and MongoDB. Despite facing challenges along the way, as a self-learner, this project has been an invaluable learning experience. It has allowed me to expand my skill set, gain exposure to relational databases and AWS services, and develop a problem-solving mindset. I am excited to continue building upon this knowledge and applying it to future projects and opportunities.</p>

<ul>
<li>Relational Database Structure: Coming from a background of working with a NoSQL database, I found it challenging to design a relational database structure using MySQL for the project. Creating a one-to-many relationship and defining foreign keys was unfamiliar territory, and I struggled initially to implement it correctly.</li>
<li>Connecting to Local MySQL Database: Setting up a connection to a local MySQL database from my Node.js application posed some challenges. I encountered issues with the configuration and authentication, which led to errors when trying to establish the connection.</li>
<li>Data Insertion and Retrieval: Adding data from Node.js to the MySQL database and retrieving existing data from the database was another area where I faced difficulties. Managing queries and handling data in a relational database required a different approach compared to the NoSQL database I had worked with previously.</li>
<li>AWS RDS Configuration: Integrating AWS RDS (Amazon Relational Database Service) with my Node.js application proved to be a significant challenge. I encountered several unsuccessful connection errors while trying to set up and configure the AWS RDS instance.</li>
<li> AWS EB Deployment: Connection Error and many other issues while trying to deploy the task through AWS.</li> 
</ul>

<h2>Thank You & Please find my resume below</h2>
<a href="https://drive.google.com/file/d/1uRZmAvieloIxX7XSpalirAaElPwWkDWc/view?usp=drive_link"><i>Resume</i></a>
