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

# how to code first to do the tables to a find and replace of the table names and then just create the variables
    # Set up db
    cur.execute('DROP DATABASE IF EXISTS "EnterDBName"');
    cur.execute('CREATE DATABASE "EnterDBName"');
    cur.execute('USE EnterDBName');

    # Drop Existing Tables
    cur.execute('DROP TABLE IF EXISTS "Player"');
    cur.execute('DROP TABLE IF EXISTS "Deliveries"');
    cur.execute('DROP TABLE IF EXISTS "Matches"');
    cur.execute('DROP TABLE IF EXISTS "Team_Stats"');
    cur.execute('DROP TABLE IF EXISTS "Team"');
    cur.execute('DROP TABLE IF EXISTS "player-Deliveries"');
    cur.execute('DROP TABLE IF EXISTS "Team-Deliveries"');
    cur.execute('DROP TABLE IF EXISTS "Deliveries-matches"');
    cur.execute('DROP TABLE IF EXISTS "Team-Matche"');
    , Team-Deliveries, Deliveries-matches,Team-Matches
    # Create Tables
    cur.execute(
        '''CREATE TABLE Player  (
        Name           VARCHAR(50) NOT NULL PRIMARY KEY,
        DOB            Date        NOT NULL PRIMARY KEY,
        Batting_Hand   VARCHAR(15),
        country        VARCHAR(30),
        Bowling_Skill  varchar(25),
        team           varchar(50)
        FOREIGN key (team) references Team(team) );''')
    cur.execute(
        '''CREATE TABLE Deliveries  (
        Match_ID          int NOT NULL PRIMARY KEY,
        inning            int          PRIMARY KEY,
        Batting_Team      VARCHAR(50) Primary key,
        Bowling_Team      VARCHAR(50) Primary key,
        Over              int Primary key,
        Ball              int Primary key,
        Batsman           Varchar(50),
        Non_Striker       Varchar(50),
        Bowler            Varchar(50)
        );''')
    cur.execute(
        '''CREATE TABLE Matches  (
        Match_ID INT NOT NULL PRIMARY KEY ,
        Season                  Varchar(50),
        City                    Varchar(50),
        Date                    Date,
        Team1                   Varchar(50),
        Team2                   Varchar(50),
        TossWnner               Varchar(50),
        TossDescision           Varchar(5),
        Result                  Varchar(8)
        );''')
    # Create Join Tables
    cur.execute(
        '''CREATE TABLE Team_Stats (
        TeamS           Varchar(50) Not NULL Primary Key,
        HomeWins        Int,
        AwayWins        Int ,
        HomeMatches     Int,
        AwayMatches     Int
        );''')
    cur.execute(
        '''CREATE TABLE Team (
        Team Varchar(20) not null Primary Key,
        teamS varchar(50)
        FOREIGN KEY (teamS) references Team_Stats(TeamS)
        );''')
#we still need to make tables for player-Deliveries, Team-Deliveries, Deliveries-matches,Team-Matches
def insert_data(cur):
        # Insertions for conditions table
    with open("data/Players.csv", 'r') as r1:
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
                'INSERT IGNORE INTO Player(,PrimaryComponent,SecondaryComponent,Additional_Information) VALUES (%s,%s,%s,%s)',
                (sample_ID, primary, secondary, additional_info))
    # insertions for Yeast-gene and Localization table and Yeast-gene&localization join table
    with open("data/deliveries.csv", 'r') as r1:
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
    with open("data/matches.csv", 'r') as r1:
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
print('hello')
cur = cnx.cursor()
setup_dp(cur)
insert_data(cur)
cur.close()
cnx.commit()
cnx.close()
