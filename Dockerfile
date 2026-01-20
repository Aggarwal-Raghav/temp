# Start with the latest Ubuntu LTS (Long Term Support)
FROM ubuntu:24.04

# Avoid interactive prompts during build (e.g. asking for timezone)
ENV DEBIAN_FRONTEND=noninteractive

# 1. Install System Dependencies & Compilers (GCC, G++, Make, OpenSSL, etc.)
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    git \
    build-essential \
    libssl-dev \
    zlib1g-dev \
    libncurses5-dev \
    libnss3-dev \
    libreadline-dev \
    libffi-dev \
    libsqlite3-dev \
    libbz2-dev \
    openjdk-17-jdk \
    && rm -rf /var/lib/apt/lists/*

# 2. Build Python 3.14.2 from Source
# (This takes a few minutes but guarantees the exact version)
WORKDIR /tmp
RUN wget https://www.python.org/ftp/python/3.14.2/Python-3.14.2.tgz \
    && tar -xf Python-3.14.2.tgz \
    && cd Python-3.14.2 \
    && ./configure --enable-optimizations \
    && make -j $(nproc) \
    && make install \
    && cd .. \
    && rm -rf Python-3.14.2*

# 4. Set Environment Variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# 5. Project Setup
WORKDIR /workspace

# Install Python requirements
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Verify installations
RUN python3 --version && java -version && gcc --version

# Keep container alive
CMD ["tail", "-f", "/dev/null"]
