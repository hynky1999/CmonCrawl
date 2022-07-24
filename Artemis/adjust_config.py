import os
from shutil import copy2, copyfile
from xml.dom import minidom
from pathlib import Path

ARTEMIS_CFG = Path(os.environ.get("ARTEMIS_PATH", "/var/lib/artemis")) / "etc"
LOGIN_CFG = Path(
    os.environ.get("ARTEMIS_CFG", Path(__file__).parent.resolve() / "config")
)

# jolokia-access.xml
def adjust_jolokia_access(mytree: minidom.Document):
    allow_origin = mytree.getElementsByTagName("allow-origin")[0].firstChild
    allow_origin.nodeValue = "*://*"
    return mytree


def adjust_broker_config(mytree: minidom.Document):
    broker_settings = minidom.parseString(
        """
    <root>
        <duplicates>
            <id-cache-size>10000000</id-cache-size>
            <persist-id-cache>true</persist-id-cache>
        </duplicates>
        <wildcard>
            <wildcard-addresses>
                <routing-enabled>true</routing-enabled>
                <delimiter>.</delimiter>
                <any-words>#</any-words>
                <single-word>*</single-word>
            </wildcard-addresses>
        </wildcard>
        <addresses>
            <address-setting match="queue.#">
                <default-address-routing-type>ANYCAST</default-address-routing-type>
                <default-queue-routing-type>ANYCAST</default-queue-routing-type>
            </address-setting>
            <address-setting match="topic.#">
                <default-address-routing-type>MULTICAST</default-address-routing-type>
                <default-queue-routing-type>MULTICAST</default-queue-routing-type>
            </address-setting>
        </addresses>
        <security>
            <security-settings>
                <security-setting match="#">
                    <permission type="createNonDurableQueue" roles="amq, producer, consumer"/>
                    <permission type="deleteNonDurableQueue" roles="amq, producer, consumer"/>
                    <permission type="createDurableQueue" roles="amq, producer, consumer"/>
                    <permission type="deleteDurableQueue" roles="amq, producer, consumer"/>
                    <permission type="createAddress" roles="amq, producer, consumer"/>
                    <permission type="deleteAddress" roles="amq, producer, consumer"/>
                    <permission type="consume" roles="amq, consumer"/>
                    <permission type="browse" roles="amq"/>
                    <permission type="send" roles="amq, producer"/>
                    <!-- we need this otherwise ./artemis data imp wouldn't work -->
                    <permission type="manage" roles="amq"/>
                </security-setting>
            </security-settings>
        </security>
    </root>
    """
    )
    core = mytree.getElementsByTagName("core")[0]
    for child in broker_settings.getElementsByTagName("duplicates")[0].childNodes:
        if isinstance(child, minidom.Element):
            core.appendChild(child.cloneNode(True))

    for child in broker_settings.getElementsByTagName("wildcard")[0].childNodes:
        if isinstance(child, minidom.Element):
            core.appendChild(child.cloneNode(True))

    core.removeChild(core.getElementsByTagName("security-settings")[0])
    for child in broker_settings.getElementsByTagName("security")[0].childNodes:
        if isinstance(child, minidom.Element):
            core.appendChild(child.cloneNode(True))

    addresses = mytree.getElementsByTagName("address-settings")[0]
    for child in broker_settings.getElementsByTagName("addresses")[0].childNodes:
        if isinstance(child, minidom.Element):
            addresses.insertBefore(child.cloneNode(True), addresses.firstChild)

    return mytree


def adjust():

    fns = [adjust_jolokia_access, adjust_broker_config]
    files = ["jolokia-access.xml", "broker.xml"]
    for i in range(len(fns)):
        mytree = fns[i](minidom.parse(str(ARTEMIS_CFG / files[i])))
        with open(str(ARTEMIS_CFG / files[i]), "w") as f:
            f.write(mytree.toprettyxml())
    # copy files from LOGIN_CFG to ARTEMIS_CFG

    for file in os.listdir(LOGIN_CFG):
        copyfile(str(LOGIN_CFG / file), str(ARTEMIS_CFG / file))


adjust()
