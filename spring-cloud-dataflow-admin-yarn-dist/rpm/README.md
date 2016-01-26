# spring-cloud-dataflow-admin-yarn-dist-rpm

Packaging and release code and artifacts for Spring Cloud Data Flow Admin YARN

## Prepare for a new version

Update `rpmbuild/SPECS/dataflow-admin-yarn.spec` with the current version.

Next, copy the Spring Cloud Data Flow Admin YARN release distribution zip file to `rpmbuild/SOURCES`

Finally, build the RPM using Vagrant.


### Building Spring Cloud Data Flow Admin YARN RPM with Vagrant

You need to install [Vagrant](http://docs.vagrantup.com/v2/installation/) and [VirtualBox](https://www.virtualbox.org/wiki/Downloads). Then add the `bento/centos-6.7` box for VirtualBox.

We are providing a Vagrant file for easy building of the RPM. Follow these steps from the root directory of this project:

    vagrant up

That should start the VM and you can now ssh to it using:

    vagrant ssh

Now, build the RPM:

    $ rpmbuild -bb rpmbuild/SPECS/dataflow-admin-yarn.spec

The RPM should now be available in `rpmbuild/RPMS/noarch`
