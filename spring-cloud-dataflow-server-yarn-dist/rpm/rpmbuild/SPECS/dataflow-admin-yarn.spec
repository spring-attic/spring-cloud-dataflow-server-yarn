# Data Flow Admin for YARN spec file for RHEL 6

Name:           spring-cloud-dataflow-admin-yarn 
Version:        1.0.0.M1
Release:        1
Summary:        Pivotal Spring Data Flow: A cloud native programming and operating model for composable data microservices on a structured platform.
# see /usr/share/doc/rpm-*/GROUPS for list
Group:          Applications/Databases
License:        Apache License v2.0
Vendor:         Pivotal
Packager:       spring-cloud@pivotal.io
URL:            http://cloud.spring.io/spring-cloud-dataflow/

# Disable automatic dependency processing
#####AutoReqProv: no

%define GROUP_NAME      pivotal
%define USER_NAME       dataflow
%define USER_GECOS      "Spring Cloud Data Flow User"
%define USER_HOME_BASE	/opt/pivotal
%define USER_HOME	%{USER_HOME_BASE}/%{USER_NAME}
%define INSTALL_DIR     /opt/pivotal/dataflow
# install dir with slash escapes for use in sed
%define INSTALL_DIR_ESC     \\\/opt\\\/pivotal\\\/dataflow

Source0:        spring-cloud-dataflow-admin-yarn-dist-%{version}.zip

BuildArch:      noarch
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

#BuildRequires:  

Requires: chkconfig

%description
Pivotal's A cloud native programming and operating model for composable data 
microservices on a structured platform. With Spring Cloud Data Flow, developers can 
create, orchestrate and refactor data pipelines through single programming model 
for common use cases such as data ingest, real-time analytics, and data import/export.


%prep
# setup, disable unpacking, create dir with name-version
# setup cmd for non-compiling pkgs
%setup -T -c %{name}-%{version}
# can prep into the build directory by doing

if [ ! -z %{_builddir} ]; then
   echo "BUILDDIR: %{_builddir}"
   # clean out the builddir
   rm -rf %{_builddir}/*
fi
echo "DEBUG: %{_builddir}"
cd %{_builddir}
###gzip -dc %{SOURCE0} | tar -xf -
unzip %{SOURCE0} 
if [ $? -ne 0 ]; then
  exit $?
fi


###%build

%pre

# create group and user account if they do not exist
if [ ! -n "`/usr/bin/getent group %{GROUP_NAME}`" ]; then
    %{_sbindir}/groupadd -r %{GROUP_NAME} 2> /dev/null
fi

if [ ! -n "`/usr/bin/getent passwd %{USER_NAME}`" ]; then
    # Do not attempt to create a home dir at this point in the RPM install
    # because the parent dirs do not yet exist and we can't be certain 
    # about /home
    %{_sbindir}/useradd -c %{USER_GECOS} -r -g %{GROUP_NAME} -M %{USER_NAME} 2> /dev/null
fi

%install
echo "entering install"
rm -rf %{buildroot}
mkdir -p %{buildroot}%{INSTALL_DIR}-%{version}

cp -rp %{_builddir}/%{name}-%{version}/* %{buildroot}%{INSTALL_DIR}-%{version}/

%clean
####echo "entering clean"

%files
####echo "entering files"

%defattr(-, %{USER_NAME}, %{GROUP_NAME} -)
%{INSTALL_DIR}-%{version}

%post

# Check if this RPM is presently installed, if so, none of this should be needed
if [ "$1" = "1" ]; then

   # add softlink
   ln -s %{INSTALL_DIR}-%{version} %{INSTALL_DIR}
   ln -s %{INSTALL_DIR}/bin/dataflow-shell %{_bindir}/dataflow-shell

fi # end if for RPM not presently installed


%preun
# If we are doing an erase
if [ "$1" = "0" ]; then

fi

# $1 == 1 @ upgrade, $1 == 0 @ uninstall
%postun
if [ "$1" = "0" ]; then
   # remove softlinks
   rm -f %{INSTALL_DIR}
   rm -f %{_bindir}/dataflow-shell
   # remove this logfile
   rm -f /tmp/spring.log
elif [ "$1" = "1" ]; then
   # On upgrade, remove the old version softlink
   rm -f %{INSTALL_DIR}
fi

# Always passed 0
%posttrans
   # This is the final scriptlet to run during an upgrade and runs from the
   # new package
   # remove the old softlink
   rm -f %{INSTALL_DIR}
   # Add the new softlink
   ln -s %{INSTALL_DIR}-%{version} %{INSTALL_DIR}
