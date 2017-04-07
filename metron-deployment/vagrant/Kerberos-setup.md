# Setting Up Kerberos in Vagrant Full Dev
**Note:** These are instructions for Kerberizing Metron Storm topologies from Kafka to Kafka. This does not cover the sensor connections or MAAS.

1. Build full dev and ssh into the machine
  ```
cd incubator-metron/metron-deployment/vagrant/full-dev-platform
vagrant up
vagrant ssh
  ```

2. Export env vars. Replace *node1* with the appropriate hosts if running anywhere other than full-dev Vagrant.
  ```
# execute as root
sudo su -
export ZOOKEEPER=node1
export BROKERLIST=node1
export HDP_HOME="/usr/hdp/current"
export METRON_VERSION="0.3.1"
export METRON_HOME="/usr/metron/${METRON_VERSION}"
  ```

3. Setup Kerberos
  ```
# Note: if you copy/paste this full set of commands, the kdb5_util command will not run as expected, so run the commands individually to ensure they all execute
# set 'node1' to the correct host for your kdc
yum -y install krb5-server krb5-libs krb5-workstation
sed -i 's/kerberos.example.com/node1/g' /etc/krb5.conf
# This step takes a moment. It creates the kerberos database.
kdb5_util create -s
/etc/rc.d/init.d/krb5kdc start
/etc/rc.d/init.d/kadmin start
chkconfig krb5kdc on
chkconfig kadmin on
  ```

4. Setup the admin user principal. Make sure to remember the password.
  ```
kadmin.local -q "addprinc admin/admin"
  ```

5. Kerberize the cluster via Ambari. More detailed documentation can be found [here](http://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.5.3/bk_security/content/_enabling_kerberos_security_in_ambari.html).

    a. For this exercise, choose existing MIT KDC (this is what we setup and installed in the previous steps.)

    ![enable keberos](readme-images/enable-kerberos.png)

    ![enable keberos get started](readme-images/enable-kerberos-started.png)

    b. Setup Kerberos configuration. Realm is EXAMPLE.COM. The admin principal will end up as admin/admin@EXAMPLE.COM when testing the KDC. Use the password you entered during the step for adding the admin principal.

    ![enable keberos configure](readme-images/enable-kerberos-configure-kerberos.png)

    c. Click through to “Start and Test Services.” Let the cluster spin up.

6. Push some sample data to one of the parser topics. E.g for yaf we took raw data from [incubator-metron/metron-platform/metron-integration-test/src/main/sample/data/yaf/raw/YafExampleOutput](../../metron-platform/metron-integration-test/src/main/sample/data/yaf/raw/YafExampleOutput)
  ```
cat sample-yaf.txt | ${HDP_HOME}/kafka-broker/bin/kafka-console-producer.sh --broker-list ${BROKERLIST}:6667 --security-protocol SASL_PLAINTEXT --topic yaf
  ```

7. Wait a few moments for data to flow through the system and then check for data in the Elasticsearch indexes. Replace yaf with whichever parser type you’ve chosen.
  ```
curl -XGET "${ZOOKEEPER}:9200/yaf*/_search"
curl -XGET "${ZOOKEEPER}:9200/yaf*/_count"
  ```

8. You should have data flowing from the parsers all the way through to the indexes. This completes the Kerberization instructions

### Other useful commands:
#### Kerberos
Unsure of your Kerberos principal associated with a keytab? There are a couple ways to get this. One is via the list of principals that Ambari provides via downloadable csv. If you didn’t download this list, you can also check the principal manually by running the following against the keytab.
```
klist -kt /etc/security/keytabs/<keytab-file-name>
```

E.g.
```
klist -kt /etc/security/keytabs/hbase.headless.keytab
Keytab name: FILE:/etc/security/keytabs/hbase.headless.keytab
KVNO Timestamp         Principal
---- ----------------- --------------------------------------------------------
   1 03/28/17 19:29:36 hbase-metron_cluster@EXAMPLE.COM
   1 03/28/17 19:29:36 hbase-metron_cluster@EXAMPLE.COM
   1 03/28/17 19:29:36 hbase-metron_cluster@EXAMPLE.COM
   1 03/28/17 19:29:36 hbase-metron_cluster@EXAMPLE.COM
   1 03/28/17 19:29:36 hbase-metron_cluster@EXAMPLE.COM
```

#### Kafka with Kerberos enabled

##### Write data to a topic with SASL
```
cat sample-yaf.txt | ${HDP_HOME}/kafka-broker/bin/kafka-console-producer.sh --broker-list ${BROKERLIST}:6667 --security-protocol PLAINTEXTSASL --topic yaf
```

##### View topic data from latest offset with SASL
```
${HDP_HOME}/kafka-broker/bin/kafka-console-consumer.sh --zookeeper ${ZOOKEEPER}:2181 --security-protocol PLAINTEXTSASL --topic yaf
```

##### View the current ACLs
```
${HDP_HOME}/kafka-broker/bin/kafka-acls.sh --authorizer kafka.security.auth.SimpleAclAuthorizer --authorizer-properties zookeeper.connect=${ZOOKEEPER}:2181 --list;
```


#### References
* [https://github.com/apache/storm/blob/master/SECURITY.md](https://github.com/apache/storm/blob/master/SECURITY.md)
