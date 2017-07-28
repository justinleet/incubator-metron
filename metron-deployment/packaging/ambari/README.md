# Ambari Management Pack Development
Typically, Ambari Management Pack development will be done in the Vagrant environments. These instructions are specific to Vagrant, but can be adapted for other environemnts (e.g. make sure to be on the correct nodes for server vs agent files)

There is an `mpack.json` file which describes what services the mpack will contains, versions, etc.

Alongside this are two directories, `addon-services` and `common-services`.

The layout of `/common-services/METRON.CURRENT` is
* `/configuration`
  * This contains a set of `*-env.xml` files, relevent to particular components or the service as a whole.  These are where properties are defined.
* `/package`
  * `/files`
    * Contains files that get used as provided, in particular Elasticsearch templates.
  * `/scripts`
    * A set of Python scripts that interface with Ambari to manage setup and install
    * `/params`
      * A set of Python scripts for managing parameters from the `*-env.xml` files
    * `/templates`
      * A set of Jinja template files which can be populated with properties
* `/quicklinks`
  * Contains `quicklinks.json` to define the Ambari quicklinks that should be used in the UI
* `themes`
  * Manages the Ambari UI themes blocks for organizing the configuration by relevant area.
* `kerberos.json`
  * Defines the keytabs and other Kerberos configuration to be used when Kerberizing a cluster
* `metainfo.xml`
  * Defines the METRON service, along with required packages, services, etc.
* `service_advisor.py`
  * Handles component layout and validation, along with handling some configurations for other services or that needs configs from other services.

The layout of `/addon-services/METRON.CURRENT` is
* `/repos`
  * Contains `repoinfo.xml` that defines repositories to install packages from
* `metainfo.xml`
  * Limited info version of `/common-services/METRON.CURRENT/metainfo.xml`
* `role_command_order.json`
  * Defines the order of service startup and other actions relative to each other.

## Adding a new property
1. Add the property to the appropriate `*-env.xml` file found in `METRON.CURRENT/configuration`.
  ```
    <property>
        <name>new_property</name>
        <description>New Property description</description>
        <value>Default Value</value>
        <display-name>New Property Pretty Name</display-name>
    </property>
  ```
The appropriate `*-env.xml` file should be selected based on which component depends on the property.  This allows Ambari to accurately restart only the affected components when the property is changed.  If a property is in `metron-env.xml`, Ambari will prompt you to restart all Metron components.

2.  Add the property to the `metron_theme.json` file found in `METRON.CURRENT/themes` if the property was added to a component-specific `*-env.xml` file (`metron-parsers-env.xml` for example) and not `metron-env.xml`.
This is necessary for the property to be displayed in the correct tab of the Metron Configs section in the Ambari UI. Using other properties as a guide, add the property to `/configuration/placement/configs` for proper placement and also to `/configuration/widgets` for a specific widget type.

3.  Reference the property in `METRON.CURRENT/package/scriptes/params/params_linux.py`, unless it will be used in Ambari's status command.  It will be stored in a variable. The name doesn't have to match, but it's preferred that it does.
Make sure to use replace `metron-env` the correct `*-env` file, as noted above.
  ```
  new_property = config['configurations']['metron-env']['new_property']
  ```
If this property will be used in the status command, instead make this change in `METRON.CURRENT/package/scriptes/params/status_params.py`.
Afterwards, in `params_linux.py`, reference the new property:
  ```
  new_property = status_params.new_property
  ```
This behavior is because Ambari doesn't send all parameters to the status, so it needs to be explicitly provided.

4. Ambari master services can then import the params:

  ```
  from params import params
  env.set_params(params)
  ```

5. The `*_commands.py` files receive the params as an input from the master services.  Once this is done, they can be accessed via the variable we set above:
  ```
  self.__params.new_property
  ```


### Env file property walkthrough
To illustrate how property files are carried through to the scripts for our services, we'll run through an existing property, `metron_apps_hdfs_dir`.

#### Defining the property
First the property appears in the appropriate `*-env.xml`, in this case `METRON.CURRENT/configuration/metron-env.xml`.
```
<property>
    <name>metron_apps_hdfs_dir</name>
    <value>/apps/metron</value>
    <description>Metron apps HDFS dir</description>
    <display-name>Metron apps HDFS dir</display-name>
</property>
```

This defines several things
1. The name of the property we'll be referencing it by in our code.
1. The default value of the property.
1. The description of the property for the Ambari UI.
1. The pretty name that will be shown in Ambari for the property.

#### Making the property available to scripts
Second, we set up the property to be available to the code. This happens in `METRON.CURRENT/packages/scripts/params/params_linux.py`. Just add the following line:
```
metron_apps_hdfs_dir = config['configurations']['metron-env']['metron_apps_hdfs_dir']
```

There is one catch to this.  If we wanted this to be available to Ambari's status command, we'd need to put that line in `METRON.CURRENT/packages/scripts/params/status_params.py` and reference it in `params_linux.py` like so:
```
metron_apps_hdfs_dir = status_params.metron_apps_hdfs_dir
```

This behavior is because Ambari doesn't send all parameters to the status, so it needs to be explicitly provided.

In our case, we don't use this parameter directly (but it could be if we wanted to use it exactly).  We actually append to it before use in `params_linux.py`:
```
from resource_management.libraries.functions import format
# ...
hdfs_grok_patterns_dir = format("{metron_apps_hdfs_dir}/patterns")
```
The `format` function is a special Ambari function that will let you create a string with properties in curly braces replaced by their values.
In this case, `hdfs_grok_patterns_dir` will be `/apps/metron/patterns` and will be what we now follow in this example.

#### Importing the parameters into scripts
`hdfs_grok_patterns_dir` is used in `METRON.CURRENT/package/scripts/parser_commands.py`, but before we can reference it, we'll need the params available to `parser_commands.py`

To make them available, we take them in as part of the `__init__`
```
def __init__(self, params):
    if params is None:
        raise ValueError("params argument is required for initialization")
    self.__params = params
    # Other initialization
```

This init is called from various Ambari service methods in `METRON.CURRENT/package/scripts/parser_master.py`, e.g.:
```
def stop(self, env, upgrade_type=None):
    from params import params
    env.set_params(params)
    commands = ParserCommands(params)
    commands.stop_parser_topologies()
```

Once the params are available to `parser_commands.py`, `hdfs_grok_patterns_dir` is by referencing `self.__params.hdfs_grok_patterns_dir)`
In our case, this will create and populate `/apps/metron/patterns` on HDFS, owned by the metron user with appropriate permissions.
It'll also log out what it's doing, which is important for being able to debug.
```
def init_parsers(self):
    Logger.info(
        "Copying grok patterns from local directory '{0}' to HDFS '{1}'".format(self.__params.local_grok_patterns_dir,
                                                                                self.__params.hdfs_grok_patterns_dir))

    self.__params.HdfsResource(self.__params.hdfs_grok_patterns_dir,
                               type="directory",
                               action="create_on_execute",
                               owner=self.__params.metron_user,
                               mode=0755,
                               source=self.__params.local_grok_patterns_dir)

    Logger.info("Done initializing parser configuration")
```

### Jinja Templates and properties
Jinja templates allow for the ability to have most of a file defined, but allow variables to be filled in from the properties defined in our `*-env.xml` files.

A variable to be replaced will take the form `{{property_name}}`

The properties are made available like any other property, and then the template itself is referenced in Python scripts in the `METRON.CURRENT/package/scripts/` directory.


### Jinja template property walkthrough
To illustrate the use of a property in a Jinja template, let's take an example of an existing property and walk through exactly how it's implemented.

A straightforward example is `metron_log_dir` in `METRON.CURRENT/configuration/metron-env.xml`

#### Defining the property
First, we need the property in the configuration file:
```
<property>
    <name>metron_log_dir</name>
    <value>/var/log/metron</value>
    <description>Log directory for metron</description>
    <display-name>Metron log dir</display-name>
</property>
```

#### Making the property available to templates
This property isn't used in Ambari's status check, so it was directly added to `METRON.CURRENT/package/scripts/params/params_linux.py`.  All we do is add the variable, and reference Ambari's config object appropriately, making sure to reference `metron-env` as the file where the property is located.
```
metron_log_dir = config['configurations']['metron-env']['metron_log_dir']
```

The property is referenced in `metron.j2`.
```
METRON_LOG_DIR="{{metron_log_dir}}"
```

#### Using the template in scripts
For that property to actually be used, it is referenced in `rest_master.py`:
```
from resource_management.core.resources.system import File
from resource_management.core.source import Template

def configure(self, env, upgrade_type=None, config_dir=None):
    # Do stuff

    env.set_params(params)
    File(format("/etc/sysconfig/metron"),
         content=Template("metron.j2")
         )
    # Do stuff
```
This will create a file on the Ambari agent machine's file system, `/etc/sysconfig/metron`, with the content of `metron.j2`, after replacing `{{metron_log_dir}}` with the value of the property (`/var/log/metron`)
```
...
METRON_LOG_DIR="/var/log/metron"
...
```

#### Defining presentation in the Ambari UI
Where and how a property is displayed can be controlled in the `METRON.CURRENT/themes/metron_theme.json` file.  Consider the `enrichment_workers` property that is defined in a component specific `*-env.xml` file, in this case `METRON.CURRENT/configuration/metron-enrichment-env.xml`.
The property appears in `METRON.CURRENT/themes/metron_theme.json` in two different sections:
```
{
  "configuration": {
    "layouts": [...],
    "placement": {
      "configs": [
        {
          "config": "metron-enrichment-env/enrichment_workers",
          "subsection-name": "subsection-enrichment-storm"
        },
        ...
      ]
    },
    "widgets": [
      {
        "config": "metron-enrichment-env/enrichment_workers",
        "widget": {
          "type": "text-field"
        }
      }
    ]
  }
}
```

The first setting places the property in the "Storm" section of the "Enrichment" tab in Ambari.  Sections are defined in `metron_theme.json` under `/configuration/layouts`.

The second setting defines a widget type of `text-field`.  See the [Ambari Wiki](https://cwiki.apache.org/confluence/display/AMBARI/Enhanced+Configs) for more detail on widget types.

If a property is defined in `metron-env.xml`, it is not necessary to add it to the `metron_theme.json` file.  By default the property will be located under the "Advanced" tab in the "Advanced metron-env" section.   

## How to identify errors in MPack changes
Typically, any errors are thrown at one of two times:

1. Attempting to install Metron as a whole.  These are typically service level definition errors, not property errors. Logs will often be found in `/var/log/ambari-server/ambari-server.log`.  Often the log will indicate it was unable to load a particular service or subservice with some more detail.
2. Running the actual functionality.  These typically tend to cause errors in the UI at runtime.  Logs are usually found in `/var/log/ambari-agent/ambari-agent.log`.

Unfortunately, because errors tend to occur at runtime, it's often necessary to add things like logging statements, or even just throw errors to print out in the Ambari UI.

The primary solution to these is to look in the logs for exceptions, see what's going wrong (Property doesn't exist?  Malformed file couldn't be loaded?), and adjust appropriately.

## Testing changes without cycling Vagrant build
There are techniques we can use to avoid spinning down and spinning back up Vagrant.

### Directly modifying files in Ambari
This assumes the installation went through, and we're just working on getting our particular feature / adjustment to work properly.

Ambari stores the Python files from the service in a couple places.  We'll want to update the files, then have Ambari pick up the updated versions and use them as the new basis.  A reinstall of Metron is unnecessary for this type of testing.

Specifically, the server files live in
```
/var/lib/ambari-server/resources/mpacks/metron-ambari.mpack-0.4.0.0/common-services
/var/lib/ambari-agent/cache/common-services
```

e.g. enrichment_commands.py can be found in:

```
/var/lib/ambari-server/resources/mpacks/metron-ambari.mpack-0.4.0.0/common-services/METRON/0.4.0/package/scripts/enrichment_commands.py
/var/lib/ambari-agent/cache/common-services/METRON/0.4.0/package/scripts/enrichment_commands.py
```

A `find` command can also be useful in quickly locating the exact location of a file, e.g.
```
[root@node1 ~]# find /var/lib/ -name enrichment_commands.py
/var/lib/ambari-server/resources/mpacks/metron-ambari.mpack-0.4.0.0/common-services/METRON/0.4.0/package/scripts/enrichment_commands.py
/var/lib/ambari-agent/cache/common-services/METRON/0.4.0/package/scripts/enrichment_commands.py
```

The steps to update are:

1. Stop Metron through Ambari.  If the property in question is used by the topologies, we'll want them stopped, so they can be restarted with the new property.
  * This can sometimes be skipped if the change doesn't affect the topologies themselves, but is a case by case choice.
1. Edit the file(s) with your changes.  The ambari-agent file must be edited, but generally better to update both for consistency.
1. Restart the Ambari Agent to get the cache to pick up the modified file
`service ambari-agent restart`
1. Start Metron through Ambari.

### Reinstalling the mpack
After we've modified files in Ambari and the mpack is working, it is a good idea to reinstall it.  Fortunately this can be done without rebuilding the Vagrant environment by following these steps:

1. Stop Metron through Ambari and remove the Metron service
1. Rebuild the mpack on your local machine, ensuring that all changes made to directly to files in Ambari were also made in your local environment
`cd metron-deployment && mvn clean package`
1. Deploy the new mpack to Vagrant
`scp packaging/ambari/metron-mpack/target/metron_mpack-0.4.0.0.tar.gz root@node1:~`
1. Log in to Vagrant
`ssh root@node1`
1. Deploy the mpack to Ambari
`ambari-server install-mpack --mpack=metron_mpack-0.4.0.0.tar.gz --verbose --force`
1. Restart Ambari
`ambari-server restart`
1. Install the mpack through Ambari as you normally would


## Configuration involving dependency services
Metron can define expectations on other services, e.g. Storm's `topology.classpath` should be `/etc/hbase/conf:/etc/hadoop/conf`.
This happens in `METRON.CURRENT/service_advisor.py`.

The value is defined in a map in `getSTORMSiteDesiredValues(self, is_secured)`.  This map is used by `validateSTORMSiteConfigurations` to ensure that the value used by the component is what we need it to be.  Generally, only the map should need to be modified.

Properties from the other services can also be used and examined.  We use this to build the appropriate URL for Storm, because it differs based on Kerberos security.
For example, to retrieve from another service, access the configurations array, retrieve the appropriate config file, retrieve the properties, and finally the desired property.
```
stormUIServerPort = services["configurations"]["storm-site"]["properties"]["ui.port"]
```

The security of the cluster can be checked with
```is_secured = self.isSecurityEnabled(services)```
Note that the configuration expectations will be properly enforced if the cluster is upgraded (e.g. the Storm URL is properly updated) and it does not need to occur manually.

## Kerberos
Any scripts interacting with underlying secured tech needs to have `kinit` run as needed.  This includes anything interacting with HDFS, HBase, Kafka, Storm, etc.
This `kinit` should be run in a conditional statement, to avoid breaking non-Kerberized clusters.

Ambari can run `kinit`, as a given user with the appropriate keytab like so:
```
if self.__params.security_enabled:
    metron_security.kinit(self.__params.kinit_path_local,
                          self.__params.metron_keytab_path,
                          self.__params.metron_principal_name,
                          execute_user=self.__params.metron_user)
```
The `security_enabled` param is already made available, along with appropriate keytabs for metron, hdfs, kafka, and hbase users.

## Best practices
* Write scripts to be idempotent. The pattern currently used is to write a file out when a task is finished, e.g. setting up ACLs or tables.
For example, when indexing is configured, a file is written out and checked based on a property.

  ```
  def set_configured(self):
      File(self.__params.indexing_configured_flag_file,
           content="",
           owner=self.__params.metron_user,
           mode=0755)
  ```
This is checked in the indexing master

  ```
  if not commands.is_configured():
      commands.init_kafka_topics()
      commands.init_hdfs_dir()
      commands.set_configured()
  ```

* Ensure ACLs are properly managed.  This includes Kafka and HBase. Often this involves a config file written out as above because this isn't idempotent!
  * Make sure to `kinit` as the correct user for setting up ACLs in a secured cluster.  This is usually kafka for Kafka and hbase for HBase.
  * See `set_hbase_acls` in `METRON.CURRENT/package/scripts/enrichment_commands.py` for an HBase example
  * See `init_kafka_acls` in `METRON.CURRENT/package/scripts/enrichment_commands.py` and  `METRON.CURRENT/package/scripts/metron_service.py` for an Kafka example
