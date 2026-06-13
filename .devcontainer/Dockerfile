FROM mcr.microsoft.com/devcontainers/python:3.11

ARG DATABRICKS_CLI_VERSION=0.296.0

RUN apt-get update && apt-get install -y --no-install-recommends \
    unzip \
    jq \
    && rm -rf /var/lib/apt/lists/*

# Azure CLI
RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash \
    && rm -rf /var/lib/apt/lists/*

# GitHub CLI
RUN curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
        | dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg \
    && chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
        | tee /etc/apt/sources.list.d/github-cli.list > /dev/null \
    && apt-get update && apt-get install -y gh \
    && rm -rf /var/lib/apt/lists/*

# Databricks CLI (Go-based)
RUN ARCH=$(dpkg --print-architecture) \
    && curl -fsSL "https://github.com/databricks/cli/releases/download/v${DATABRICKS_CLI_VERSION}/databricks_cli_${DATABRICKS_CLI_VERSION}_linux_${ARCH}.zip" \
        -o /tmp/databricks.zip \
    && unzip /tmp/databricks.zip -d /usr/local/bin/ databricks \
    && rm /tmp/databricks.zip

# Node.js 22 LTS + Claude Code
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/* \
    && npm install -g @anthropic-ai/claude-code

# Python packages
COPY requirements.txt /tmp/local-dev-requirements.txt
RUN pip install --no-cache-dir -r /tmp/local-dev-requirements.txt
