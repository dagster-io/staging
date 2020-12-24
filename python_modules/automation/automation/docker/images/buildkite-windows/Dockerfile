ARG BASE_IMAGE
FROM "${BASE_IMAGE}"

LABEL maintainer="Elementl"

# Copied from python_modules/dagster/dev-requirements.txt
ADD build_cache/dev-requirements.txt dev-requirements.txt


SHELL ["powershell", "-command"]

# https://stackoverflow.com/a/43681637
RUN Invoke-WebRequest 'https://github.com/git-for-windows/git/releases/download/v2.12.2.windows.2/MinGit-2.12.2.2-64-bit.zip' -OutFile MinGit.zip
RUN Expand-Archive c:\MinGit.zip -DestinationPath c:\MinGit; \
    $env:PATH = $env:PATH + ';C:\MinGit\cmd\;C:\MinGit\cmd'; \
    Set-ItemProperty -Path 'HKLM:\SYSTEM\CurrentControlSet\Control\Session Manager\Environment\' -Name Path -Value $env:PATH

RUN pip install -U pip setuptools wheel
RUN pip install -r dev-requirements.txt

ENTRYPOINT [ "powershell" ]
