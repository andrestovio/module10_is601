# **Module 9: Databases and Web Applications**

## **Module Overview**

Welcome to **Module 9**, where we delve deep into the world of **databases and web applications**. In this module, you will work with the **FastAPI Calculator** application, enhancing it to interact with a **PostgreSQL** database using **SQLAlchemy**. You will also utilize **Faker** to seed the database with realistic data. Additionally, you will create a **users** table and establish a one-to-many relationship between users and their calculations, demonstrating fundamental database relationships.

By the end of this module, you will have a comprehensive understanding of setting up and managing databases within web applications, writing and optimizing SQL queries, leveraging ORM tools to streamline database operations, and implementing basic relational mappings. This knowledge will empower you to build robust, scalable, and efficient web applications that effectively manage and utilize data.

---

## **Why Databases in Web Applications?**

Databases are the backbone of modern web applications, enabling the storage, retrieval, and management of data essential for application functionality. Understanding how databases integrate with web applications is crucial for building scalable and efficient software solutions. This module introduces you to the fundamental concepts and technologies that facilitate this integration, including:

- **Relational Databases:** Systems like **PostgreSQL** that store data in structured tables with predefined schemas.
- **SQL (Structured Query Language):** The standard language for querying and managing relational databases.
- **Object-Relational Mapping (ORM) with SQLAlchemy:** Tools that allow developers to interact with databases using Pythonic syntax, abstracting the underlying SQL.
- **Containerization with Docker Compose:** Techniques to set up and manage multi-service applications, ensuring consistent environments across different stages of development and deployment.
- **Basic Database Relationships:** Understanding one-to-many relationships to model real-world data interactions.

By mastering these technologies, you'll be equipped to design, implement, and maintain databases that power dynamic and data-driven web applications.

---

## **Learning Outcomes**

By the end of this module, you will be able to:

1. **Set Up a Development Environment Using Docker Compose for Web Applications and Databases.**
2. **Develop a REST API using Python and FastAPI Integrated with a Relational Database.**
3. **Understand and Compare Relational Database Systems: PostgreSQL vs. MySQL.**
4. **Write and Optimize SQL Queries for Data Manipulation and Retrieval.**
5. **Interact with Databases Using SQLAlchemy ORM in Python Applications.**
6. **Implement Professional Terminology and Concepts Related to Database Management and Web Development.**
7. **Configure and Manage Database Services Using pgAdmin.**
8. **Leverage Docker for Containerizing Web Applications and Databases to Ensure Consistent Development Environments.**
9. **Apply Best Practices for Database Design, Query Optimization, and Data Integrity.**
10. **Utilize Git for Version Control and Collaborative Development.**
11. **Set Up GitHub Actions for Continuous Integration (CI), Automating the Execution of Tests to Ensure Software Quality.**
12. **Seed Databases with Realistic Data Using Python Scripts and Faker.**
13. **Establish One-to-Many Relationships Between Database Tables.**

---

## **Learning Pathway**

### **Recall**

**Title:** Introduction to Databases and Web Applications  
**Grading Type:** Points  
**Instructions:**  

1. **Reflect on Previous Knowledge:**
   - Consider how data is stored and retrieved in the applications you've used.
   - Think about your understanding of client-server architecture and data exchange mechanisms.

2. **Participate in a Discussion:**
   - Share your thoughts on the role of databases in web applications:
     - Why are databases essential for web applications?
     - What types of databases have you encountered before?
     - How do you think Python interacts with databases in web applications?

**Purpose:** This activity will help you connect your existing knowledge with new concepts, preparing you for the upcoming material on databases and their integration with web applications.

---

### **Read**

1. **[Introduction to FastAPI - Comprehensive Reference](https://fastapi.tiangolo.com/tutorial/#run-the-code)**  
   *Purpose:* Learn the basics of FastAPI, a modern Python web framework for building APIs.

2. **[Understanding Relational Databases](https://www.geeksforgeeks.org/relational-database-management-system-rdbms/)**  
   *Purpose:* Gain a foundational understanding of relational databases, their structure, and how they manage data.

3. **[Docker and Docker Compose Documentation](https://docs.docker.com/compose/)**  
   *Purpose:* Understand how to containerize applications and manage multi-container setups using Docker Compose.

4. **[PostgreSQL Official Documentation](https://www.postgresql.org/docs/)**  
   *Purpose:* Explore the features and functionalities of PostgreSQL, an advanced open-source relational database system.

5. **[MySQL Official Documentation](https://dev.mysql.com/doc/)**  
   *Purpose:* Learn about MySQL, its features, and how it differs from other RDBMS like PostgreSQL.

6. **[SQL (Structured Query Language) Tutorial](https://www.w3schools.com/sql/)**  
   *Purpose:* Acquire the skills to write and understand SQL queries for managing and manipulating relational databases.

7. **[SQLAlchemy Documentation](https://www.sqlalchemy.org/documentation/)**  
   *Purpose:* Discover how to use SQLAlchemy for interacting with databases using Python, leveraging its ORM capabilities.

8. **[PostgreSQL vs. MySQL: Feature Comparison](https://www.guru99.com/postgresql-vs-mysql.html)**  
   *Purpose:* Understand the differences, strengths, and use-cases for PostgreSQL and MySQL to make informed database choices.

---

### **Watch**

You are required to watch the instructional video provided on Canvas for this unit. 

---

## **Step-by-Step Guide: Setting Up and Managing Databases in Your Web Application**

This guide will walk you through enhancing the **FastAPI Calculator** web application by integrating it with a **PostgreSQL** database using **SQLAlchemy**. You will also create a **users** table and establish a one-to-many relationship between users and their calculations. Additionally, you will create a Python script to seed the database with realistic data using **Faker**. This process will help you understand how to manage database interactions, maintain data integrity, and automate data population for testing and development purposes.

---

### **Step 1: Understanding Databases and Their Integration with Web Applications**

#### **1.1 What is a Relational Database?**

A relational database is a type of database that stores and provides access to data points that are related to one another. Data is organized into tables, which consist of rows and columns, allowing for easy data management and retrieval.

**Key Components:**

- **Tables:** Structured collections of data entries consisting of rows and columns.
- **Schemas:** Define the structure of the database, including tables, fields, and relationships.
- **Primary Keys:** Unique identifiers for each record in a table.
- **Foreign Keys:** Fields that create a link between two tables, enforcing referential integrity.

#### **1.2 Importance of Databases in Web Applications**

Databases store essential data required by web applications to function, such as user information, product details, transactions, and more. Efficient database management ensures that data is easily accessible, secure, and scalable to meet the application's demands.

**Benefits:**

- **Data Persistence:** Ensures that data remains intact and accessible even when the application is not running.
- **Data Integrity:** Maintains accuracy and consistency of data through constraints and relationships.
- **Scalability:** Supports growing amounts of data and increased user interactions without compromising performance.

#### **1.3 Introduction to PostgreSQL and MySQL**

**PostgreSQL** and **MySQL** are two powerful open-source relational database management systems (RDBMS) widely used in web applications.

- **PostgreSQL:**
  - Known for its advanced features, extensibility, and compliance with SQL standards.
  - Supports complex queries, transactions, and a wide range of data types.
  - Ideal for applications requiring robust data integrity and scalability.

- **MySQL:**
  - Widely adopted for its speed, reliability, and ease of use.
  - Supports various storage engines, with InnoDB being the default for transactional support.
  - Suitable for applications needing high-speed operations and a large ecosystem of tools.

**Brief Comparison:**

While both PostgreSQL and MySQL are robust RDBMS options, PostgreSQL offers more advanced features and extensibility, making it suitable for complex applications requiring advanced data handling. MySQL, on the other hand, is renowned for its speed and simplicity, making it a popular choice for applications where these factors are paramount.

---

### **Step 2: Exploring the FastAPI Calculator Application with Docker Compose**

#### **2.1 Application Structure**

- **`docker-compose.yml`:** Defines the services required for the application: FastAPI web service, PostgreSQL database, and pgAdmin.
- **`Dockerfile`:** Instructions to build the FastAPI application container.
- **`main.py`:** Defines the FastAPI application, including API endpoints and serving the HTML page.
- **`app/operations.py`:** Contains arithmetic functions (`add`, `subtract`, `multiply`, `divide`).
- **`templates/index.html`:** The HTML frontend that interacts with the API.
- **`requirements.txt`:** Lists the Python dependencies required for the project.
- **`tests/`:** Directory containing unit, integration, and end-to-end tests.
- **`seed_db.py`:** Python script to seed the database with fake data using Faker.
- **`app/models.py`:** Defines SQLAlchemy models for `User` and `Calculation`.
- **`app/database.py`:** Configures the database connection and session management.

#### **2.2 Understanding Docker and Docker Compose in the Application**

- **Docker:** Packages the FastAPI application and its dependencies into containers, ensuring consistency across different environments.
- **Docker Compose:** Orchestrates multiple containers (web, db, pgadmin) using the `docker-compose.yml` file, simplifying the setup and management of the development environment.

**Benefits:**

- **Isolation:** Each service runs in its own container, preventing conflicts and ensuring stability.
- **Reproducibility:** Ensures that the application runs consistently across different machines and environments.
- **Scalability:** Easily scale services up or down based on application needs.

---

### **Step 3: Setting Up the Development Environment**

Follow these steps to set up the FastAPI Calculator project on your local machine:

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/your-username/fastapi-calculator.git
   cd fastapi-calculator
   ```
   *Replace `your-username` with the actual GitHub username if applicable.*

2. **Review the `docker-compose.yml` File:**
   Ensure that the `docker-compose.yml` file is present in the root directory of the project. This file defines the services required for the application:
   - **web:** The FastAPI application.
   - **db:** The PostgreSQL database.
   - **pgadmin:** The pgAdmin interface for database management.

3. **Build and Start the Services:**
   Use Docker Compose to build the Docker images and start the containers:
   ```bash
   docker-compose up --build
   ```
   
   **Explanation of the Command:**
   - `docker-compose`: The Docker Compose command-line tool.
   - `up`: Builds, (re)creates, starts, and attaches to containers for a service.
   - `--build`: Forces the rebuilding of the Docker images.

   **What Happens Next:**
   - **Web Service:** Builds the FastAPI application from the current directory and starts the server on port `8000`.
   - **Database Service:** Pulls the PostgreSQL image, sets up the database with the specified credentials, and starts the service on port `5432`.
   - **pgAdmin Service:** Pulls the pgAdmin image and starts the web interface on port `5050`.

4. **Access the FastAPI Application:**
   Once the services are up and running, you can interact with the FastAPI application:
   - **API Documentation:** Navigate to [http://localhost:8000/docs](http://localhost:8000/docs) to access the automatically generated Swagger UI documentation.
   - **Root Endpoint:** Access [http://localhost:8000](http://localhost:8000) to verify that the API is running.

5. **Access pgAdmin:**
   pgAdmin provides a graphical interface to manage your PostgreSQL database:
   - **URL:** [http://localhost:5050](http://localhost:5050)
   - **Login Credentials:**
     - **Email:** `admin@example.com`
     - **Password:** `admin`

6. **Configure pgAdmin to Connect to PostgreSQL:**
   After logging into pgAdmin:
   1. **Add a New Server:**
      - Click on "Add New Server."
   2. **General Tab:**
      - **Name:** `FastAPI PostgreSQL`
   3. **Connection Tab:**
      - **Host:** `db` *(As defined in `docker-compose.yml`)*
      - **Port:** `5432`
      - **Maintenance Database:** `fastapi_db`
      - **Username:** `postgres`
      - **Password:** `postgres`
   4. **Save:** Click "Save" to establish the connection.

   You should now see the `fastapi_db` database in pgAdmin, allowing you to manage tables, run queries, and perform administrative tasks.

7. **Stopping the Services:**
   To stop the running containers, press `CTRL + C` in the terminal where Docker Compose is running, then execute:
   ```bash
   docker-compose down
   ```
   This command stops and removes the containers, networks, and volumes defined in the `docker-compose.yml` file.

---

### **Step 4: Interacting with the Database Using SQL and SQLAlchemy**

#### **4.1 Writing and Optimizing SQL Queries**

**SQL (Structured Query Language)** is the standard language for managing and manipulating relational databases. It allows you to perform various operations, such as querying data, updating records, and managing database structures.

**Core SQL Operations:**

- **Data Querying:** `SELECT` statements to retrieve data.
- **Data Manipulation:** `INSERT`, `UPDATE`, and `DELETE` statements.
- **Data Definition:** `CREATE`, `ALTER`, and `DROP` statements for managing database objects.
- **Data Control:** `GRANT` and `REVOKE` statements for permissions.

**Example SQL Queries:**

- **Creating Tables with Relationships:**
  ```sql
  CREATE TABLE users (
      id SERIAL PRIMARY KEY,
      username VARCHAR(50) NOT NULL UNIQUE,
      email VARCHAR(100) NOT NULL UNIQUE,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE calculations (
      id SERIAL PRIMARY KEY,
      operation VARCHAR(20) NOT NULL,
      operand_a FLOAT NOT NULL,
      operand_b FLOAT NOT NULL,
      result FLOAT NOT NULL,
      timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      user_id INTEGER NOT NULL,
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
  );
  ```

- **Inserting Data:**
  ```sql
  INSERT INTO users (username, email) VALUES ('john_doe', 'john@example.com');

  INSERT INTO calculations (operation, operand_a, operand_b, result, user_id) 
  VALUES ('add', 2, 3, 5, 1);
  ```

- **Querying Data with Relationships:**
  ```sql
  SELECT users.username, calculations.operation, calculations.result
  FROM calculations
  JOIN users ON calculations.user_id = users.id
  WHERE users.username = 'john_doe';
  ```

- **Updating Data:**
  ```sql
  UPDATE calculations 
  SET result = 6 
  WHERE id = 1;
  ```

- **Deleting Data:**
  ```sql
  DELETE FROM calculations 
  WHERE id = 1;
  ```

**Optimizing SQL Queries:**

- **Indexing:** Creating indexes on frequently queried columns (e.g., `username`, `operation`) to speed up data retrieval.
- **Joins:** Efficiently using joins to combine data from multiple tables.
- **Avoiding SELECT *:** Specifying only the necessary columns to reduce data transfer and processing time.
- **Using Prepared Statements:** Enhancing performance and security by reusing query plans.

#### **4.2 Interacting with the Database Using SQLAlchemy**

**SQLAlchemy** is a comprehensive SQL toolkit and Object-Relational Mapping (ORM) library for Python. It allows developers to interact with databases using Python classes and objects instead of writing raw SQL queries.

**Key Features:**

- **ORM Capabilities:** Maps Python classes to database tables, enabling object-oriented database interactions.
- **SQL Expression Language:** Facilitates the construction of complex SQL queries programmatically.
- **Database Abstraction:** Supports multiple database backends, making it easier to switch between different RDBMS.
- **Session Management:** Handles transactions and connections efficiently.

**Example Usage:**

- **Defining Models with Relationships:**
  ```python
  # app/models.py

  from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey
  from sqlalchemy.orm import relationship
  from sqlalchemy.ext.declarative import declarative_base
  from datetime import datetime

  Base = declarative_base()

  class User(Base):
      __tablename__ = 'users'
      
      id = Column(Integer, primary_key=True, index=True)
      username = Column(String(50), unique=True, nullable=False)
      email = Column(String(100), unique=True, nullable=False)
      created_at = Column(DateTime, default=datetime.utcnow)
      
      calculations = relationship("Calculation", back_populates="owner", cascade="all, delete-orphan")

  class Calculation(Base):
      __tablename__ = 'calculations'
      
      id = Column(Integer, primary_key=True, index=True)
      operation = Column(String(20), nullable=False)
      operand_a = Column(Float, nullable=False)
      operand_b = Column(Float, nullable=False)
      result = Column(Float, nullable=False)
      timestamp = Column(DateTime, default=datetime.utcnow)
      user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
      
      owner = relationship("User", back_populates="calculations")
  ```

- **Creating a Session:**
  ```python
  # app/database.py

  from sqlalchemy import create_engine
  from sqlalchemy.orm import sessionmaker

  DATABASE_URL = "postgresql://postgres:postgres@db:5432/fastapi_db"

  engine = create_engine(DATABASE_URL)
  SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
  ```

- **Performing CRUD Operations:**
  ```python
  # app/crud.py

  from sqlalchemy.orm import Session
  from app.models import User, Calculation

  def create_user(db: Session, username: str, email: str):
      db_user = User(username=username, email=email)
      db.add(db_user)
      db.commit()
      db.refresh(db_user)
      return db_user

  def get_user(db: Session, user_id: int):
      return db.query(User).filter(User.id == user_id).first()

  def create_calculation(db: Session, operation: str, operand_a: float, operand_b: float, result: float, user_id: int):
      db_calculation = Calculation(
          operation=operation,
          operand_a=operand_a,
          operand_b=operand_b,
          result=result,
          user_id=user_id
      )
      db.add(db_calculation)
      db.commit()
      db.refresh(db_calculation)
      return db_calculation

  def get_calculations_by_user(db: Session, user_id: int):
      return db.query(Calculation).filter(Calculation.user_id == user_id).all()
  ```

**Benefits of Using SQLAlchemy:**

- **Productivity:** Simplifies database interactions, reducing the need to write repetitive SQL code.
- **Maintainability:** Enhances code readability and maintainability through abstraction.
- **Flexibility:** Offers both high-level ORM and low-level SQL expression capabilities, allowing developers to choose the appropriate level of abstraction.

---

### **Step 5: Comparing PostgreSQL and MySQL**

Choosing the right relational database management system (RDBMS) is crucial for the success of your application. **PostgreSQL** and **MySQL** are two of the most popular open-source RDBMS options. Understanding their strengths and weaknesses will help you make informed decisions based on your project's requirements.

#### **PostgreSQL**

**Pros:**

- **Advanced Features:**
  - **Full ACID Compliance:** Ensures reliable transactions with Atomicity, Consistency, Isolation, and Durability.
  - **Complex Queries:** Supports window functions, Common Table Expressions (CTEs), and recursive queries.
  - **Extensibility:** Ability to define custom data types, operators, and functions.
  - **Robust JSON Support:** Facilitates both relational and non-relational data handling.

- **Standards Compliance:**
  - Adheres closely to SQL standards, promoting portability and consistency across different environments.

- **Performance Optimization:**
  - Efficient handling of large datasets with advanced indexing options like B-tree, Hash, GIN, and GiST.
  - Supports parallel queries and table partitioning for enhanced performance.

- **Strong Community and Ecosystem:**
  - Active open-source community contributing to a rich set of extensions and tools, such as PostGIS for geospatial data.

- **Reliability and Stability:**
  - Proven track record in production environments, especially for mission-critical applications.

**Cons:**

- **Learning Curve:**
  - Its extensive feature set can be overwhelming for beginners or those accustomed to simpler RDBMSs.

- **Resource Consumption:**
  - May require more memory and CPU resources compared to some alternatives for optimal performance.

- **Replication Setup:**
  - While robust, setting up advanced replication (e.g., logical replication) can be more complex.

#### **MySQL**

**Pros:**

- **High Performance:**
  - Optimized for speed, particularly in read-heavy operations.
  - Efficient query processing and indexing for fast data retrieval.

- **Wide Adoption:**
  - Extensive community support and a vast ecosystem of third-party tools and integrations.
  - Popular in the LAMP (Linux, Apache, MySQL, PHP/Python/Perl) stack.

- **Flexible Storage Engines:**
  - Supports multiple storage engines (e.g., InnoDB for transactions, MyISAM for read-heavy operations) allowing customization based on application needs.

- **Open Source and Licensing:**
  - Fully open-source under the GNU General Public License (GPL), ensuring no proprietary restrictions.

- **Ease of Use:**
  - Generally easier to set up and administer, making it accessible for beginners.
  - Comprehensive documentation and a plethora of tutorials available.

**Cons:**

- **Less Advanced Features:**
  - Lacks some of PostgreSQL's advanced SQL compliance and extensibility options.
  - Limited support for certain data types and complex queries.

- **Replication and Clustering:**
  - While it offers replication solutions, they may not be as feature-rich or robust as PostgreSQL's options.

- **JSON Support:**
  - Present but not as mature or feature-rich as PostgreSQL’s implementation, potentially limiting flexibility for JSON-heavy applications.

---

### **Step 6: Best Practices for Database Design and Management**

#### **6.1 Database Normalization**

- **Purpose:** Organize database schema to reduce redundancy and improve data integrity.
- **Forms:**
  - **First Normal Form (1NF):** Eliminate duplicate columns and create separate tables for related data.
  - **Second Normal Form (2NF):** Remove subsets of data that apply to multiple rows and place them in separate tables.
  - **Third Normal Form (3NF):** Remove columns that are not dependent on the primary key.

#### **6.2 Indexing Strategies**

- **Benefits:** Speeds up data retrieval operations.
- **Types:**
  - **Primary Index:** Automatically created on the primary key.
  - **Secondary Index:** Created on other columns frequently used in queries (e.g., `username`, `operation`).
  - **Composite Index:** Index on multiple columns to optimize complex queries.

#### **6.3 Query Optimization**

- **Techniques:**
  - **Use EXPLAIN:** Analyze how queries are executed to identify bottlenecks.
  - **Avoid SELECT *:** Specify only the necessary columns to reduce data transfer.
  - **Limit Use of Joins:** Use joins judiciously and ensure they are optimized.
  - **Use Prepared Statements:** Enhance performance and security by reusing query plans.

#### **6.4 Ensuring Data Integrity**

- **Constraints:**
  - **NOT NULL:** Ensure that a column cannot have NULL values.
  - **UNIQUE:** Ensure all values in a column are unique.
  - **CHECK:** Ensure that all values in a column satisfy a specific condition.
  - **FOREIGN KEY:** Maintain referential integrity between tables, enforcing relationships like one-to-many.

#### **6.5 Regular Backups and Maintenance**

- **Importance:** Protects against data loss and ensures database reliability.
- **Strategies:**
  - **Automated Backups:** Schedule regular backups using tools like `pg_dump` for PostgreSQL or `mysqldump` for MySQL.
  - **Monitoring:** Use monitoring tools to track database performance and health.
  - **Updates:** Keep the database software up-to-date with the latest security patches and features.

---

### **Step 7: Utilizing Git for Version Control**

Version control is crucial for managing changes to your codebase and collaborating with others.

1. **Initialize Git Repository:**
   ```bash
   git init
   ```

2. **Create a `.gitignore` File:**
   - Include common patterns to exclude, such as virtual environment directories and compiled files.
   - Example `.gitignore` content:
     ```
     venv/
     __pycache__/
     *.pyc
     *.pyo
     *.pyd
     .env
     ```

3. **Commit Changes Regularly:**
   ```bash
   git add .
   git commit -m "Initial commit with application code and tests"
   ```

4. **Push to GitHub:**
   ```bash
   git remote add origin https://github.com/your-username/fastapi-calculator.git
   git push -u origin main
   ```
   *Replace `your-username` with your actual GitHub username.*

---

### **Step 8: Setting Up Continuous Integration with GitHub Actions**

Automating testing ensures that your code remains reliable as you make changes.

1. **Create Workflow File:**

   - **Path:** `.github/workflows/ci.yml`

2. **Workflow Configuration:**

   ```yaml
   name: CI

   on: [push, pull_request]

   jobs:
     build:
       runs-on: ubuntu-latest

       services:
         db:
           image: postgres
           env:
             POSTGRES_USER: postgres
             POSTGRES_PASSWORD: postgres
             POSTGRES_DB: fastapi_db
           ports:
             - 5432:5432
           options: >-
             --health-cmd pg_isready
             --health-interval 10s
             --health-timeout 5s
             --health-retries 5

       steps:
       - uses: actions/checkout@v2

       - name: Set up Python
         uses: actions/setup-python@v2
         with:
           python-version: '3.9'

       - name: Install dependencies
         run: |
           pip install -r requirements.txt
           pip install pytest sqlalchemy faker
           pip install playwright
           playwright install

       - name: Wait for PostgreSQL
         run: |
           until pg_isready -h localhost -p 5432; do
             sleep 1
           done

       - name: Run Unit Tests
         run: pytest tests/unit/

       - name: Run Integration Tests
         run: pytest tests/integration/

       - name: Run End-to-End Tests
         run: pytest tests/e2e/
   ```

3. **Commit and Push Workflow File:**
   ```bash
   git add .github/workflows/ci.yml
   git commit -m "Add CI workflow for automated testing"
   git push
   ```

4. **Verify Workflow Execution:**
   - Check the **Actions** tab on your GitHub repository to see the workflow run and ensure all tests pass successfully.

---

### **Step 9: Implementing Logging and Monitoring**

Logging is essential for tracking application behavior, diagnosing issues, and maintaining performance.

1. **Configure Logging in `main.py`:**
   
   ```python
   import logging

   logging.basicConfig(level=logging.INFO)
   logger = logging.getLogger(__name__)
   ```

2. **Add Logging Statements:**
   
   ```python
   from app.database import SessionLocal
   from app.crud import create_calculation

   @app.post("/add", response_model=OperationResponse, responses={400: {"model": ErrorResponse}})
   async def add_route(operation: OperationRequest):
       """
       Add two numbers.
       """
       try:
           result = add(operation.a, operation.b)
           logger.info(f"Adding {operation.a} and {operation.b} with result {result}")
           
           # Assume user_id is obtained from authentication (to be implemented in future module)
           user_id = 1  # Placeholder for demonstration
           
           # Store calculation in the database
           db = SessionLocal()
           calculation = create_calculation(db, 'add', operation.a, operation.b, result, user_id)
           db.close()
           
           return OperationResponse(result=result)
       except Exception as e:
           logger.error(f"Add Operation Error: {str(e)}")
           raise HTTPException(status_code=400, detail=str(e))
   ```

3. **Verify Logs:**
   - Run the application and perform operations via the API or frontend to observe logs in the console.
   - Ensure logs provide useful information about operations and any potential errors.

4. **Advanced Logging with Log Levels:**
   - Utilize different log levels (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`) to categorize log messages appropriately.
   
   ```python
   logger.debug("This is a debug message")
   logger.info("This is an info message")
   logger.warning("This is a warning message")
   logger.error("This is an error message")
   logger.critical("This is a critical message")
   ```

5. **Integrate Monitoring Tools (Optional):**
   - Consider integrating tools like **Prometheus** and **Grafana** for advanced monitoring and visualization of application metrics.

---

## **Hands-On Assignment**

**Title:** Build and Manage Databases in Your Web Application  
**Grading Type:** Points  

**Assignment Instructions:**  

1. **Set Up the Project:**
   - Use the provided code for the **FastAPI Calculator** application.
   - Ensure all dependencies are installed and the application runs locally using Docker Compose.

2. **Implement SQLAlchemy:**
   - Define SQLAlchemy models to represent `User` and `Calculation`.
   - Update `main.py` to interact with the PostgreSQL database using SQLAlchemy.
   - Establish a one-to-many relationship between `User` and `Calculation`.
   - Ensure that each arithmetic operation (`add`, `subtract`, `multiply`, `divide`) stores the calculation details in the `calculations` table associated with a user.

3. **Create a Database Seeder with Faker:**
   - Install the **Faker** library:
     ```bash
     pip install faker
     ```
   - Create a Python script named `seed_db.py` that uses Faker to generate and insert a significant number of users and their corresponding calculation records into the database.
   - Example `seed_db.py`:
     ```python
     # seed_db.py

     from sqlalchemy.orm import Session
     from app.models import Base, User, Calculation
     from app.database import engine, SessionLocal
     from faker import Faker
     import random

     def seed_database(db: Session, num_users: int = 50, calculations_per_user: int = 20):
         fake = Faker()
         operations = ['add', 'subtract', 'multiply', 'divide']
         for _ in range(num_users):
             username = fake.unique.user_name()
             email = fake.unique.email()
             user = User(username=username, email=email)
             db.add(user)
             db.commit()
             db.refresh(user)
             
             for _ in range(calculations_per_user):
                 operation = random.choice(operations)
                 a = round(random.uniform(1, 100), 2)
                 b = round(random.uniform(1, 100), 2)
                 if operation == 'add':
                     result = a + b
                 elif operation == 'subtract':
                     result = a - b
                 elif operation == 'multiply':
                     result = a * b
                 elif operation == 'divide':
                     result = a / b if b != 0 else 0
                 calculation = Calculation(
                     operation=operation,
                     operand_a=a,
                     operand_b=b,
                     result=result,
                     user_id=user.id
                 )
                 db.add(calculation)
         db.commit()
         print(f"Seeded {num_users} users and {num_users * calculations_per_user} calculations.")

     if __name__ == "__main__":
         Base.metadata.create_all(bind=engine)
         db = SessionLocal()
         seed_database(db, num_users=50, calculations_per_user=20)
         db.close()
     ```
   - Ensure that running `python seed_db.py` populates the `users` and `calculations` tables with realistic data.

4. **Enhance Logging:**
   - Implement logging throughout the application to track database operations and any potential errors.
   - Ensure that logs provide meaningful insights into the application's behavior.

5. **Implement Best Practices for Database Design:**
   - Apply normalization techniques to your database schema.
   - Create appropriate indexes to optimize query performance (e.g., indexing `username` in `users` and `operation` in `calculations`).
   - Ensure data integrity through the use of constraints and relationships.

6. **Utilize Git for Version Control:**
   - Commit changes regularly with descriptive messages.
   - Use branches if working on different features or database optimizations.

7. **Set Up Continuous Integration:**
   - Ensure that GitHub Actions is configured to run your tests automatically on each push.
   - Verify that the CI workflow includes all types of tests and passes successfully.

8. **Write Tests:**
   - **Unit Tests:** Test individual functions and methods in your application.
   - **Integration Tests:** Test the interaction between FastAPI routes and the database.
   - **End-to-End Tests:** Simulate user interactions with the web interface and verify the end-to-end functionality.

9. **Submit:**
   - **GitHub Repository Link:** Provide the link to your repository containing the application code, database schema, seeder script, and tests.
   - **Screenshots:**
     - Successful GitHub Actions workflow run.
     - Database setup and management in pgAdmin.
     - Application running in the browser displaying the calculator interface.

---

## **Reflect**

**Title:** Module 9 Reflection  
**Grading Type:** Points  
**Instructions:**  

Compose a reflection (600-700 words) on your experience setting up and managing databases within your web application. Address the following points:

1. **Understanding Databases:**
   - Discuss how integrating PostgreSQL with FastAPI enhanced your understanding of relational databases.

2. **Challenges and Solutions:**
   - Identify challenges you faced, such as setting up Docker Compose, configuring the database, writing SQL queries, or using SQLAlchemy.
   - Explain how you overcame these challenges.

3. **Database Design and Optimization:**
   - Reflect on your experience designing the database schema and optimizing queries.
   - Discuss the importance of normalization, indexing, and query optimization.

4. **Comparing PostgreSQL and MySQL:**
   - Compare your experiences using PostgreSQL and MySQL.
   - Highlight the strengths and weaknesses of each based on your project requirements.

5. **Testing and Continuous Integration:**
   - Reflect on how writing tests and setting up GitHub Actions contributed to the reliability and maintainability of your application.
   - Discuss the benefits of automated testing in your development workflow.

6. **Logging and Monitoring:**
   - Explain how implementing logging helped in tracking application behavior and diagnosing issues.
   - Consider the role of monitoring tools in maintaining application performance.

7. **Version Control with Git:**
   - Describe how using Git supported your development process.
   - Reflect on the benefits of committing changes regularly and managing branches effectively.

8. **Future Applications:**
   - Consider how the skills learned can be applied to more complex projects involving databases.
   - Reflect on areas where you seek to improve or learn more, such as advanced SQL features, database security, or scaling databases.

---

## **Quiz**

**Title:** Databases and Web Applications Quiz  
**Grading Type:** Points  

**Instructions:**

1. **Complete the Quiz:** Access the quiz on Canvas, which tests your understanding of databases, SQL, Docker Compose, FastAPI integration, SQLAlchemy, and ORM concepts.

2. **Quiz Content:** The quiz will cover:

   - Fundamental concepts of relational databases and their role in web applications.
   - Differences and comparisons between PostgreSQL and MySQL.
   - Writing and optimizing SQL queries for data manipulation and retrieval.
   - Using SQLAlchemy ORM for interacting with databases in Python applications.
   - Setting up and managing databases using Docker Compose.
   - Best practices for database design, indexing, and query optimization.
   - Integrating databases with FastAPI for building robust web applications.
   - Understanding and implementing logging and monitoring for databases.
   - Utilizing Git and GitHub Actions for version control and continuous integration.

**Question Types:**

- **Multiple-Choice:** Test your knowledge of key concepts and terminologies.
- **Short Answer:** Explain specific functionalities or code snippets.
- **Code Analysis:** Review code examples and identify errors or improvements.
- **Scenario-Based Questions:** Apply your knowledge to hypothetical project scenarios.

---

## **Supplementary Materials**

To support your learning, refer to the following resources:

- **[FastAPI Documentation](https://fastapi.tiangolo.com/)**  
  *Comprehensive guide on building APIs with FastAPI.*

- **[Docker Documentation](https://docs.docker.com/)**  
  *Official documentation for Docker and Docker Compose.*

- **[PostgreSQL Official Documentation](https://www.postgresql.org/docs/)**  
  *Detailed information on PostgreSQL features and usage.*

- **[MySQL Official Documentation](https://dev.mysql.com/doc/)**  
  *Guide to using MySQL, its features, and configurations.*

- **[pgAdmin Documentation](https://www.pgadmin.org/docs/)**  
  *Guide to using pgAdmin for PostgreSQL database management.*

- **[SQL Tutorial by W3Schools](https://www.w3schools.com/sql/)**  
  *Interactive tutorials on SQL syntax and commands.*

- **[SQLAlchemy Documentation](https://www.sqlalchemy.org/documentation/)**  
  *Comprehensive resources for using SQLAlchemy in Python applications.*

- **[PostgreSQL vs. MySQL: Feature Comparison](https://www.guru99.com/postgresql-vs-mysql.html)**  
  *In-depth comparison of PostgreSQL and MySQL features and use-cases.*

- **Books:**
  - *"Docker Deep Dive"* by Nigel Poulton
  - *"FastAPI: Modern Web APIs with Python"* by Sebastián Ramírez
  - *"PostgreSQL: Up and Running"* by Regina O. Obe and Leo S. Hsu
  - *"MySQL Cookbook"* by Paul DuBois
  - *"Essential SQLAlchemy"* by Jason Myers and Rick Copeland
  - *"SQL in 10 Minutes, Sams Teach Yourself"* by Ben Forta
  - *"PostgreSQL Administration Cookbook"* by Simon Riggs and Gianni Ciolli

---

## **Tips for Success**

- **Start Early:** Begin setting up your environment and exploring the code as soon as possible.
- **Understand the Basics:** Ensure you grasp fundamental concepts of databases and web application integration before diving into advanced topics.
- **Ask Questions:** If you're unsure about something, don't hesitate to reach out to instructors or peers.
- **Practice SQL:** Regularly write and optimize SQL queries to enhance your proficiency.
- **Leverage ORM Tools:** Utilize SQLAlchemy to simplify database interactions and reduce the need for repetitive SQL code.
- **Automate Seeding:** Use Python scripts and Faker to automate database seeding, ensuring consistent and realistic data for testing and development.
- **Utilize Resources:** Refer to the supplementary materials to deepen your knowledge and troubleshoot issues.
- **Reflect on Learning:** Take time to think about how each part of the module contributes to your overall understanding of databases and web applications.
- **Stay Organized:** Keep your codebase clean and well-documented to facilitate easier testing and maintenance.
- **Leverage Git:** Make regular commits with clear messages to track your progress and changes effectively.
- **Implement Best Practices:** Apply best practices for database design, query optimization, and data integrity to build robust applications.

---

## **Submission Deadline**

Please ensure that your GitHub repository link and reflection are submitted by **[Insert Deadline Here]**. Late submissions may be subject to grade penalties as per the course policy.

---

**Good luck with your assignment!** Embrace this opportunity to build and manage databases within your web application, and enjoy the journey of expanding your technical expertise.
