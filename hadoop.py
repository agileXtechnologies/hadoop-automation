#hadoop-automation with python
#importing the libraries
import os
import subprocess as sp
import paramiko as pm

#Hadoop class containes all the method
#like namenode_config, datanode_config
#clientnode_config 
class Hadoop:
	#primary constructor...
	#initializing variables...
	def __init__(self, hostname, username, password):
		self.hostname=hostname
		self.username=username
		self.password=password
		self.client = pm.SSHClient()
		self.client.set_missing_host_key_policy(pm.AutoAddPolicy())
		self.client.connect(hostname=hostname, username=username, password=password)

	#software download links
	java_link = "http://anfadmin.ucsd.edu/linux/RHEL/7/x86_64/jdk-8u171-linux-x64.rpm"
	hadoop_link = "archive.apache.org/dist/hadoop/core/hadoop-1.2.1/hadoop-1.2.1-1.x86_64.rpm"

	#will check whether the sofware...
	#is installed or not...
	def software_check(self, software_name):
		session = self.client.get_transport().open_channel(kind='session')
		session.exec_command("{0} -version".format(software_name))
		return session.recv_exit_status()

	#download the software
	def get_software(self, link):
		session = self.client.get_transport().open_channel(kind='session')
		session.exec_command("wget {0}".format(link))
		return session.recv_exit_status()

	#installing the software
	def install_software(self, software):
		session = self.client.get_transport().open_channel(kind='session')
		session.exec_command("rpm -i {0} --force".format(link))
		return session.recv_exit_status()

	#folder creation
	def folder(self, folder_name):
		session = self.client.get_transport().open_channel(kind='session')
		session.exec_command("mkdir /{0}".format(folder_name))
		return session.recv_exit_status()

	#hdfs-site.xml configuration
	def hdfs_config(self, type_name):
		code = '<?xml version="1.0"?>\n<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>\n\n<!-- Put site-specific property overrides in this file. -->\n\n<configuration>\n<property>\n<name>dfs.'+type_name+'.dir</name>\n<value>/masternode</value>\n</property>\n</configuration>'
		session = self.client.get_transport().open_channel(kind='session')
		session.exec_command("echo '{0}' > /etc/hadoop/hdfs-site.xml".format(code))
		return session.recv_exit_status()

	#core-site.xml configuration
	def core_config(self, ip_address):
		code = '<?xml version="1.0"?>\n<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>\n\n<!-- Put site-specific property overrides in this file. -->\n\n<configuration>\n<property>\n<name>fs.default.name</name>\n<value>hdfs://'+ip_address+':9001</value>\n</property>\n</configuration>'
		session = self.client.get_transport().open_channel(kind='session')
		session.exec_command("echo '{0}' > /etc/hadoop/core-site.xml".format(code))
		return session.recv_exit_status()

	#folder creation
	def format_node(self):
		session = self.client.get_transport().open_channel(kind='session')
		session.exec_command("hadoop namenode -format")
		return session.recv_exit_status()

	#start the node
	def start_node(self, type_node):
		session = self.client.get_transport().open_channel(kind='session')
		session.exec_command("hadoop-daemon.sh start {0}".format(type_node))
		return session.recv_exit_status()

	#setting up namenode
	def namenode_setup(self):
		print("Configuring namenode...")
		#java software...
		if(self.software_check("java") != 0):
			print("Downloading java...")
			if(self.get_software(java_link) != 0):
				print("[X] Unable to download the software!!")
				exit()
			else:
				print("[✔] Java has been downloaded")
				if(self.install_software("jdk-8u171-linux-x64.rpm") != 0):
					print("[X] Unable to install!!")
					exit()
				else:
					print("[✔] Java has been installed")
		else:
			print("[✔] Java is upto date")

		#hadoop software...
		if(self.software_check("hadoop") != 0):
			print("Downloading hadoop...")
			if(self.get_software(java_link) != 0):
				print("[X] Unable to download the Hadoop!!")
				exit()
			else:
				print("[✔] Hadoop has been downloaded")
				if(self.install_software("hadoop-1.2.1-1.x86_64.rpm") != 0):
					print("[X] Unable to install!!")
					exit()
				else:
					print("[✔] Hadoop has been installed")
		else:
			print("[✔] Hadoop is upto date")	

		#folder creation
		if(self.folder("masternode") != 0):
			print("[X] Unable to create directory!!")
		else:
			print("[✔] Folder has been created")

		#hdfs-site.xml configuration
		if(self.hdfs_config("name") != 0):
			print("[X] Unable to configure hdfs-site.xml!!")
		else:
			print("[✔] hdfs-site.xml has been configured")

		#core-site.xml configuration
		if(self.core_config(self.hostname) != 0):
			print("[X] Unable to configure core-site.xml!!")
		else:
			print("[✔] core-site.xml has been configured")

		'''
		#format the namenode
		if(self.format_node() != 0):
			print("[x] Unable to format the node!!")
		else:
			print("[✔] Namenode has been formatted")
		'''

		#Starting namenode
		if(self.start_node("namenode") != 0):
			print("[x] Unable to start the node!!")
		else:
			print("[✔] Node has been started")
	

	#setting up datanode
	def datanode_setup(self, ip_address):
		print("Configuring datanode...")
		#java software...
		if(self.software_check("java") != 0):
			print("Downloading java...")
			if(self.get_software(java_link) != 0):
				print("[X] Unable to download the software!!")
				exit()
			else:
				print("[✔] Java has been downloaded")
				if(self.install_software("jdk-8u171-linux-x64.rpm") != 0):
					print("[X] Unable to install!!")
					exit()
				else:
					print("[✔] Java has been installed")
		else:
			print("[✔] Java is upto date")

		#hadoop software...
		if(self.software_check("hadoop") != 0):
			print("Downloading hadoop...")
			if(self.get_software(java_link) != 0):
				print("[X] Unable to download the Hadoop!!")
				exit()
			else:
				print("[✔] Hadoop has been downloaded")
				if(self.install_software("hadoop-1.2.1-1.x86_64.rpm") != 0):
					print("[X] Unable to install!!")
					exit()
				else:
					print("[✔] Hadoop has been installed")
		else:
			print("[✔] Hadoop is upto date")	

		#folder creation
		if(self.folder("namenode") != 0):
			print("[X] Unable to create directory!!")
		else:
			print("[✔] Folder has been created")

		#hdfs-site.xml configuration
		if(self.hdfs_config("data") != 0):
			print("[X] Unable to configure hdfs-site.xml!!")
		else:
			print("[✔] hdfs-site.xml has been configured")

		#core-site.xml configuration
		if(self.core_config(ip_address) != 0):
			print("[X] Unable to configure core-site.xml!!")
		else:
			print("[✔] core-site.xml has been configured")

		#Starting datanode
		if(self.start_node("datanode") != 0):
			print("[x] Unable to start the node!!")
		else:
			print("[✔] Node has been started")

	#setting up clientnode
	def clientnode_setup(self, ip_address):
		print("Configuring clientnode...")
		#java software...
		if(self.software_check("java") != 0):
			print("Downloading java...")
			if(self.get_software(java_link) != 0):
				print("[X] Unable to download the software!!")
				exit()
			else:
				print("[✔] Java has been downloaded")
				if(self.install_software("jdk-8u171-linux-x64.rpm") != 0):
					print("[X] Unable to install!!")
					exit()
				else:
					print("[✔] Java has been installed")
		else:
			print("[✔] Java is upto date")

		#hadoop software...
		if(self.software_check("hadoop") != 0):
			print("Downloading hadoop...")
			if(self.get_software(java_link) != 0):
				print("[X] Unable to download the Hadoop!!")
				exit()
			else:
				print("[✔] Hadoop has been downloaded")
				if(self.install_software("hadoop-1.2.1-1.x86_64.rpm") != 0):
					print("[X] Unable to install!!")
					exit()
				else:
					print("[✔] Hadoop has been installed")
		else:
			print("[✔] Hadoop is upto date")	

		#core-site.xml configuration
		if(self.core_config(ip_address) != 0):
			print("[X] Unable to configure core-site.xml!!")
		else:
			print("[✔] core-site.xml has been configured")
