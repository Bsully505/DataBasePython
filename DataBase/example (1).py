import pymysql as ps



# name db = yeast-transcriptomes
# user_name = admin
# password: qJ6D2qwzjhbdwic
# make the connection to the db
def make_connection():
    return ps.connect(host='bryandbruby.cucbdvc0bgxn.us-east-1.rds.amazonaws.com', user='admin',
                      passwd='admin123',
                      port=3306, autocommit=True)
def setup_dp(cur):

    # Set up db
    cur.execute('DROP DATABASE IF EXISTS yeast_transcriptomesDB');
    cur.execute('CREATE DATABASE yeast_transcriptomesDB');
    cur.execute('USE yeast_transcriptomesDB');

    # Drop Existing Tables
    cur.execute('DROP TABLE IF EXISTS Conditions_Annotations');
    cur.execute('DROP TABLE IF EXISTS Yeast_Gene');
    cur.execute('DROP TABLE IF EXISTS Localization');
    cur.execute('DROP TABLE IF EXISTS SC_Expression');
    cur.execute('DROP TABLE IF EXISTS YeastGene_Localization');
    # Create Tables
    cur.execute(
        '''CREATE TABLE Conditions_Annotations (Condit_ID VARCHAR(30) NOT NULL PRIMARY KEY,PrimaryComponent VARCHAR(30),SecondaryComponent VARCHAR(30),Additional_Information VARCHAR(30));''')
    cur.execute(
        '''CREATE TABLE Yeast_Gene (Gene_ID VARCHAR(30) NOT NULL PRIMARY KEY,Validation VARCHAR(30),Biological_Process VARCHAR(30),Cellular_Component VARCHAR(30),Molecular_Function VARCHAR(30));''')
    cur.execute(
        '''CREATE TABLE Localization (Localization_ID INT NOT NULL PRIMARY KEY,Biological_Process_Loc VARCHAR(30),Cellular_Component_Loc VARCHAR(30),Molecular_Function VARCHAR(30));''')
    # Create Join Tables
    cur.execute(
        '''CREATE TABLE SC_Expression (SC_ID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,Gene_ID VARCHAR(30) NOT NULL ,Condit_ID VARCHAR(30) NOT NULL, FOREIGN KEY(Gene_ID) REFERENCES Yeast_Gene(Gene_ID),FOREIGN KEY(Condit_ID) REFERENCES Conditions_Annotations(Condit_ID),SC_Expression FLOAT);''')
    cur.execute(
        '''CREATE TABLE YeastGene_Localization (Gene_local_ID INT NOT NULL AUTO_INCREMENT PRIMARY KEY, Gene_ID VARCHAR(30) NOT NULL, Localization_ID INT NOT NULL , FOREIGN KEY(Gene_ID) REFERENCES Yeast_Gene(Gene_ID),FOREIGN KEY(Localization_ID) REFERENCES Localization(Localization_ID));''')


def insert_data(cur):
        # Insertions for conditions table
    with open("data/rf_conditions_annotations.csv", 'r') as r1:
        # skips first line the headers
        next(r1)
        for line in r1:
            line = line.split(',')
            ID_num = line.__getitem__(0)
            sample_ID = line.__getitem__(1)
            primary = line.__getitem__(2)
            secondary = line.__getitem__(3)
            additional_info = line.__getitem__(4)
            # print(ID_num,sample_ID,primary,secondary,additional_info)
            cur.execute(
                'INSERT IGNORE INTO Conditions_Annotations(Condit_ID,PrimaryComponent,SecondaryComponent,Additional_Information) VALUES (%s,%s,%s,%s)',
                (sample_ID, primary, secondary, additional_info))
    # insertions for Yeast-gene and Localization table and Yeast-gene&localization join table
    with open("data/combined_BP_CC_MF.csv", 'r') as r1:
        # skips first line the headers
        next(r1)
        for line in r1:
            line = line.split(',')
            ID_num = int(line.__getitem__(0)) + 1
            ID_num = str(ID_num)
            gene = line.__getitem__(1)
            valid = line.__getitem__(2)
            bp = line.__getitem__(3)
            cc = line.__getitem__(4)
            mf = line.__getitem__(5)
            cur.execute(
                'INSERT IGNORE INTO Yeast_Gene(Gene_ID,Validation, Biological_Process,Cellular_Component,Molecular_Function) VALUES (%s,%s,%s,%s,%s)',
                (gene, valid, bp, cc, mf))
            cur.execute(
                'INSERT IGNORE INTO Localization(Localization_ID,Biological_Process_Loc,Cellular_Component_Loc,Molecular_Function) VALUES (%s,%s,%s,%s)',
                (ID_num, bp, cc, mf))
            cur.execute('INSERT IGNORE INTO YeastGene_Localization(Gene_ID,Localization_ID) VALUES (%s,%s)', (gene, ID_num))
    # insertion for SC expression table
    with open("data/rf_SC_expressions.csv", 'r') as r1:
        next(r1)
        for line in r1:
            line = line.split(',')
            ID_num = int(line.__getitem__(0)) + 1
            ID_num = str(ID_num)
            gene = line.__getitem__(1)
            condit = line.__getitem__(2)
            sc = line.__getitem__(3)
            cur.execute('INSERT IGNORE INTO SC_Expression(Gene_ID,Condit_ID,SC_Expression) VALUES (%s,%s,%s)',
                        (gene, condit, sc))
cnx = make_connection()
cur = cnx.cursor()
setup_dp(cur)
insert_data(cur)
cur.close()
cnx.commit()
cnx.close()
