#!/bin/bash

# Download and install rustup with default options
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# Source the cargo environment script
. "$HOME/.cargo/env"

# Install development tools and required libraries
sudo yum -y groupinstall "Development Tools"
sudo yum -y install cmake
sudo yum -y install cyrus-sasl-devel
sudo yum -y install openssl-devel

