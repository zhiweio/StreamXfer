FROM amazonlinux:latest

LABEL maintainer="Wang Zhiwei <noparking188@gmail.com>"


# Install necessary packages for locales
RUN yum update -y && \
    yum clean metadata && \
    yum install -y glibc-langpack-en

ENV LANG=en_US.UTF-8 \
    LANGUAGE=en_US:en \
    LC_ALL=en_US.UTF-8

RUN locale


# Install Python 3.9
RUN yum -y install \
      wget \
      tar \
      gzip \
      make \
      gcc \
      openssl-devel \
      bzip2-devel \
      libffi-devel \
      sqlite-devel \
      zip \
      unzip \
      lzop \
      git \
      which \
      zlib-devel

WORKDIR /opt

RUN wget https://www.python.org/ftp/python/3.9.7/Python-3.9.7.tgz
RUN tar xzf Python-3.9.7.tgz

WORKDIR /opt/Python-3.9.7

RUN ./configure --enable-optimizations
RUN make altinstall

WORKDIR /

RUN rm -rf /opt/Python-3.9.7*

RUN ln -s $(which python3.9) /usr/local/bin/python3
RUN python3 -m pip install -U pip


# Install mssql-tools
RUN curl https://packages.microsoft.com/config/rhel/8/prod.repo > /etc/yum.repos.d/mssql-release.repo && \
    yum remove mssql-tools unixODBC-utf16 unixODBC-utf16-devel && \
    ACCEPT_EULA=Y yum install -y mssql-tools18 unixODBC-devel
RUN echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
RUN yum clean all


# Install AWS CLI
WORKDIR /opt

RUN yum remove awscli
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip

RUN ./aws/install

WORKDIR /

RUN rm -rf /opt/aws*


# Install StreamXfer
RUN python3 -m pip install -U git+https://github.com/zhiweio/streamxfer@master


CMD ["/bin/bash"]
