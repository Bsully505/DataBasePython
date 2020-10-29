import pymysql as ps

#AUTHORS BRYAN SULLIVAN and HENOK KELSELA


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
        Match_ID          Int NOT NULL PRIMARY KEY,
        inning            Int          PRIMARY KEY,
        Batting_Team      VARCHAR(50) PRMARY KEY,
        Bowling_Team      VARCHAR(50) PRIMARY KEY,
        Over              Int PRIMARY KEY,
        Ball              Int PRIMARY KEY,
        Batsman           Varchar(50),
        Non_Striker       Varchar(50),
        Bowler            Varchar(50)
        );''')
    cur.execute(
        '''CREATE TABLE Matches  (
        Match_ID INT NOT NULL PRIMARY KEY ,
        Season                  Varchar(50),
        City                    Varchar(50),
        Date                    DATE,
        Team1                   Varchar(50),
        Team2                   Varchar(50),
        TossWnner               Varchar(50),
        TossDescision           Varchar(5),
        Result                  Varchar(8)
        );''')
    # Create Join Tables
    cur.execute(
        '''CREATE TABLE Team_Stats (
        TeamS           Varchar(50) NOT NULL PRMARY KEY,
        HomeWins        Int,
        AwayWins        Int ,
        HomeMatches     Int,
        AwayMatches     Int
        );''')
    cur.execute(
        '''CREATE TABLE Team (
        Team       Varchar(20) NOT NULL PRIMARY KEY,
        teamS      Varchar(50)
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
            PlayerName = line.__getitem__(0)
            DOB = line.__getitem__(1)
            Batting_Hand = line.__getitem__(2)
            Bowling_Skill = line.__getitem__(3)
            Country = line.__getitem__(4)

            cur.execute(
                'INSERT IGNORE INTO Players VALUES (%s,%s,%s,%s,%s)',
                (PlayerName, DOB, Batting_Hand, Bowling_Skill, country))
            # print(ID_num,sample_ID,primary,secondary,additional_info)
    # insertions for Yeast-gene and Localization table and Yeast-gene&localization join table
    with open("data/deliveries.csv", 'r') as r1:
        # skips first line the headers
        next(r1)
        for line in r1:
            line = line.split(',')
            Match_ID = int(line.__getitem__(0))
            Match_ID = str(ID_num)
            inning = int(line.__getitem__(1))
            inning = str(ID_num)
            Batting_team = line.__getitem__(2)
            Bowling_Team = line.__getitem__(3)
            over = int(line.__getitem__(4))
            over = str(over)
            Ball = int(line.__getitem__(5))
            Ball = str(Ball)
            batsman = line.__getitem__(6)
            non_striker = line.__getitem__(7)
            bowler = line.__getitem__(8)

            cur.execute(
                'INSERT IGNORE INTO Players VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',
                (Match_ID, inning, Batting_team, Bowling_team, over, ball, batsman, non_striker, bowler))
            #cur.execute(
            #    'INSERT IGNORE INTO Localization(Localization_ID,Biological_Process_Loc,Cellular_Component_Loc,Molecular_Function) VALUES (%s,%s,%s,%s)',
            #    (ID_num, bp, cc, mf))
            #cur.execute('INSERT IGNORE INTO YeastGene_Localization(Gene_ID,Localization_ID) VALUES (%s,%s)', (gene, ID_num))
    # insertion for SC expression table
    with open("data/matches.csv", 'r') as r1:
        next(r1)
        for line in r1:
            line = line.split(',')
            ID_num = int(line.__getitem__(0))
            ID_num = str(ID_num)
            season = line.__getitem__(1)
            city = line.__getitem__(2)
            date = line.__getitem__(3)
            team1 = line.__getitem__(4)
            team2 = line.__getitem__(5)
            toss_winner = line.__getitem__(6)
            toss_decision = line.__getitem__(7)
            result = line.__getitem__(8)

            cur.execute('INSERT IGNORE INTO matches VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                        (ID_num, season, city, date, team1,team2,toss_winner,toss_decision,result));
cnx = make_connection()
cur = cnx.cursor()
setup_dp(cur)
insert_data(cur)
cur.close()
cnx.commit()
cnx.close()
