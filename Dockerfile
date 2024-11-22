# ---------------------------------------------
# Dockerfile for FastAPI Calculator Application with sudo Access
# ---------------------------------------------

# 1. Use an Official Python Runtime as a Parent Image
FROM mcr.microsoft.com/playwright/python:v1.47.0-noble

# -------------------------------------------------
# Explanation:
# - `FROM python:3.10-slim`: Specifies the base image for the Docker container.
# - The `python:3.10-slim` image is a lightweight version of Python 3.10, which reduces the overall size of the Docker image.
# - Using official images ensures reliability and security, as they are maintained and regularly updated.
# -------------------------------------------------

# 2. Set Environment Variables
# -------------------------------------------------
# These environment variables configure Python's behavior within the container.
# -------------------------------------------------
ENV PYTHONDONTWRITEBYTECODE=1 
ENV PYTHONUNBUFFERED=1        

# -------------------------------------------------
# Explanation:
# - `ENV`: Sets environment variables that persist during the build and runtime of the Docker container.
# - `PYTHONDONTWRITEBYTECODE=1`: Disables the generation of `.pyc` files, which are bytecode caches. This keeps the container's filesystem clean and reduces unnecessary disk usage.
# - `PYTHONUNBUFFERED=1`: Forces Python to run in unbuffered mode, ensuring that logs and outputs are immediately visible, which is especially useful for debugging and monitoring.
# -------------------------------------------------

# 3. Set Work Directory
WORKDIR /app

# -------------------------------------------------
# Explanation:
# - `WORKDIR /app`: Sets the working directory inside the Docker container to `/app`.
# - All subsequent commands (`RUN`, `COPY`, `CMD`, etc.) will be executed relative to this directory.
# - Organizing the application files within a designated directory improves maintainability and clarity.
# -------------------------------------------------

# 4. Install System Dependencies and sudo
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        sudo \
        passwd \
    && rm -rf /var/lib/apt/lists/*

# -------------------------------------------------
# Explanation:
# - `RUN`: Executes commands in a new layer on top of the current image and commits the results.
# - `apt-get update`: Updates the package lists to retrieve the latest version information.
# - `apt-get install -y --no-install-recommends build-essential sudo passwd`: Installs essential build tools, `sudo`, and `passwd`.
#   - `build-essential`: Required for compiling any Python packages that need compilation.
#   - `sudo`: Allows users to execute commands with superuser privileges.
#   - `passwd`: Enables setting passwords for users.
# - `--no-install-recommends`: Prevents the installation of unnecessary packages, reducing the image size and potential vulnerabilities.
# - `rm -rf /var/lib/apt/lists/*`: Cleans up the local repository of retrieved package files to minimize the image size.
# -------------------------------------------------

# 5. Create a Non-Root User and Group with sudo Privileges
RUN addgroup --system appgroup \
    && adduser --system --ingroup appgroup --disabled-password appuser \
    && echo "appuser:test" | chpasswd \
    && echo "appuser ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# -------------------------------------------------
# Explanation:
# - `addgroup --system appgroup`: Creates a system group named `appgroup`. System groups have lower GID numbers and are typically used for system services.
# - `adduser --system --ingroup appgroup --disabled-password appuser`: Creates a system user named `appuser` assigned to the `appgroup` group with no initial password.
# - `echo "appuser:YourSecurePasswordHere" | chpasswd`: Sets the password for `appuser`. Replace `YourSecurePasswordHere` with a strong, secure password.
# - `echo "appuser ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers`: Grants `appuser` password-less `sudo` privileges.
# 
# **Security Note**:
# - Embedding passwords directly in the Dockerfile is **not secure**, especially if the Dockerfile is stored in version control systems.
# - Consider using **Docker Secrets** or **Build Arguments** to handle sensitive information more securely.
# -------------------------------------------------

# 6. Install Python Dependencies
# -------------------------------------------------
# Copy only `requirements.txt` first to leverage Docker's caching mechanism.
# -------------------------------------------------
COPY requirements.txt .

# -------------------------------------------------
# Explanation:
# - `COPY requirements.txt .`: Copies the `requirements.txt` file from the host into the current working directory (`/app`) in the container.
# - By copying dependencies first, Docker can cache this layer and avoid reinstalling dependencies unless `requirements.txt` changes, speeding up subsequent builds.
# -------------------------------------------------

# Upgrade pip and Install Dependencies
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# -------------------------------------------------
# Explanation:
# - `RUN pip install --upgrade pip`: Upgrades `pip` to the latest version within the container to ensure compatibility and access to the latest features.
# - `RUN pip install -r requirements.txt`: Installs Python packages listed in `requirements.txt`, which are necessary for the FastAPI application to run.
# -------------------------------------------------

# 7. Copy the Rest of the Application Code
COPY . .

# -------------------------------------------------
# Explanation:
# - `COPY . .`: Copies all files and directories from the current directory on the host into the current working directory (`/app`) in the container.
# - This includes the application code, configuration files, and any other necessary assets.
# - Ensures that the container has all the required files to run the application.
# -------------------------------------------------

# 8. Change Ownership of the Application Directory
RUN chown -R appuser:appgroup /app

# -------------------------------------------------
# Explanation:
# - `chown -R appuser:appgroup /app`: Recursively changes the ownership of the `/app` directory to the `appuser` user and `appgroup` group.
# - Ensures that the non-root user has the necessary permissions to read, write, and execute files within the application directory.
# - Enhances security by restricting access to application files to only the designated user.
# -------------------------------------------------

# 9. Expose the Port That the App Runs On
EXPOSE 8000

# -------------------------------------------------
# Explanation:
# - `EXPOSE 8000`: Informs Docker that the container listens on port 8000 at runtime.
# - While `EXPOSE` does not publish the port itself, it serves as documentation and allows Docker to map ports correctly when using networking features.
# - It's essential to expose the port that the application uses to accept incoming connections.
# -------------------------------------------------

# 10. Install Playwright (if needed)
RUN playwright install

# -------------------------------------------------
# Explanation:
# - `RUN playwright install`: Installs necessary browsers for Playwright if your application relies on it.
# - Ensure that `playwright` is listed in your `requirements.txt` or adjust accordingly based on your application's needs.
# -------------------------------------------------

# 11. Define the Default Command to Run the Application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

# -------------------------------------------------
# Explanation:
# - `CMD`: Specifies the default command to run when the container starts.
# - `["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]`: Executes Uvicorn, an ASGI server, to run the FastAPI application.
#   - `main:app`: Points to the `app` instance in the `main.py` file, which is the FastAPI application.
#   - `--host 0.0.0.0`: Binds the server to all network interfaces within the container, making it accessible externally.
#   - `--port 8000`: Sets the port on which the server will listen inside the container.
# -------------------------------------------------

# 12. Switch to the Non-Root User
USER appuser

# -------------------------------------------------
# Explanation:
# - `USER appuser`: Switches the user context to `appuser` from this point onward in the Dockerfile.
# - Ensures that the application runs with non-root privileges, adhering to security best practices.
# - Limits the permissions available to the application, reducing the potential impact of vulnerabilities.
# -------------------------------------------------

# ---------------------------------------------
# Summary of Workflow and Security Enhancements
# ---------------------------------------------

# - **Base Image Selection**:
#   - Utilizes a lightweight and official Python image to ensure reliability and security.
#   - Reduces the overall size of the Docker image, enhancing build times and resource efficiency.

# - **Environment Configuration**:
#   - Sets environment variables to optimize Python's behavior for containerized environments.
#   - Prevents unnecessary file generation and ensures real-time logging.

# - **System Dependencies Installation**:
#   - Installs only essential build tools and `sudo`, minimizing the attack surface and reducing image size.
#   - Cleans up after installation to reduce the image size and eliminate potential vulnerabilities from unnecessary packages.

# - **Security Best Practices**:
#   - **Non-Root User with sudo Access**: Creates and utilizes a non-root user (`appuser`) with `sudo` privileges. This allows administrative tasks while still maintaining a security boundary.
#   - **Password Management**: Sets a password for `appuser` to enable `sudo` operations. **Note**: Embedding passwords in Dockerfiles is insecure and should be avoided in production environments.
#   - **Ownership Management**: Assigns appropriate ownership of application files to the non-root user, ensuring that the application has the necessary permissions without granting excessive access.

# - **Layer Caching Optimization**:
#   - Copies and installs dependencies first to leverage Docker's layer caching, speeding up subsequent builds.
#   - Ensures that only necessary files are included in the final image, enhancing performance and security.

# - **Application Execution**:
#   - Defines a clear and secure command to run the FastAPI application using Uvicorn.
#   - Binds the application to all network interfaces and exposes the required port, ensuring accessibility while maintaining isolation within the container.

# - **Comprehensive Documentation**:
#   - Detailed comments provide clarity on each instruction's purpose, aiding in learning and maintenance.
#   - Serves as an educational resource for understanding Dockerfile syntax and best practices.

# ---------------------------------------------
# Security Considerations and Best Practices
# ---------------------------------------------