#!/bin/bash
VERSION=1.58.2
OS=linux
origin=https://github.com/datacharmer/dbdeployer/releases/download/v$VERSION
wget $origin/dbdeployer-$VERSION.$OS.tar.gz
tar -xzf dbdeployer-$VERSION.$OS.tar.gz
chmod +x dbdeployer-$VERSION.$OS
sudo mv dbdeployer-$VERSION.$OS /usr/local/bin/dbdeployer
