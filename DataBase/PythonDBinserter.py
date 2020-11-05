import pymysql as ps

#AUTHORS BRYAN SULLIVAN and HENOK KETSELA


def make_connection():
    return ps.connect(host='bryandbruby.cucbdvc0bgxn.us-east-1.rds.amazonaws.com', user='admin',
                      passwd='admin123',
                      port=3306, autocommit=True)
def setup_dp(cur):

# how to code first to do the tables to a find and replace of the table names and then just create the variables
    # Set up db
    cur.execute('DROP DATABASE IF EXISTS IPL_DATA_SET');
    cur.execute('CREATE DATABASE IPL_DATA_SET');
    cur.execute('USE IPL_DATA_SET');

    # Drop Existing Tables
    cur.execute('DROP TABLE IF EXISTS Player');
    cur.execute('DROP TABLE IF EXISTS Deliveries');
    cur.execute('DROP TABLE IF EXISTS Matches');
    cur.execute('DROP TABLE IF EXISTS Team');
    cur.execute('DROP TABLE IF EXISTS player_Deliveries');
    cur.execute('DROP TABLE IF EXISTS Team_Deliveries');
    cur.execute('DROP TABLE IF EXISTS Deliveries_Matches');
    cur.execute('DROP TABLE IF EXISTS Team_Matches');

    # Create Tables
    cur.execute('''CREATE TABLE Team(TeamName Varchar(20) NOT NULL PRIMARY KEY, HomeWins Int,AwayWins Int, HomeMatches Int, AwayMatches Int);''')
    cur.execute('''CREATE TABLE Player(Name VARCHAR(50) NOT NULL PRIMARY KEY,DOB VarChar(50) NOT NULL,Batting_Hand VARCHAR(15),Bowling_Skill varchar(25) not null, country VARCHAR(30),team varchar(50) references Team(TeamName) );''')
    cur.execute('''CREATE TABLE Deliveries(Match_ID Int NOT NULL, inning Int, Batting_Team VARCHAR(50), Bowling_Team VARCHAR(50), OverNum Int, Ball Int, Batsman Varchar(50), Non_Striker Varchar(50), Bowler Varchar(50), Primary Key(Match_ID, inning, Batting_Team, Bowling_Team, OverNum, Ball));''')
    cur.execute('''Create Table Matches (MatchID int NOT Null auto_increment Primary key, Season Varchar(50), City Varchar(50), Date Varchar(20), Team1 Varchar(50), Team2 Varchar(50), Tosswinner Varchar(50), Tossdecision Varchar(50), Result VarChar(8));''')
    cur.execute('''create Table Deliveries_Matches(Match_ID int Not Null references Matches(Match_ID), inning int not null references Deliveries(inning), Batting_team Varchar(50) not null references Deliveries(Batting_team), Bowling_team varchar(50) not null references Deliveries(Bowling_team), Overnum int not null references Deliveries(overnum), Ball int not null references Deliveries(ball), Primary Key (Match_ID, inning, Batting_team, Bowling_team, Overnum, ball));''')
    cur.execute('''Create Table Player_Deliveries (Team Varchar(20) Not Null references Player(Name), DOB date REFERENCES Player(DOB), Match_ID int Not Null REFERENCES Deliveries(Match_ID), inning int REFERENCES Deliveries(inning), batting_team varchar(50) REFERENCES Deliveries(batting_team), bowling_team varchar(50) REFERENCES Deliveries(bowling_team), overnum int REFERENCES Deliveries(overnum), ball int REFERENCES Deliveries(ball), Primary Key (Team, DOB, Match_ID, inning, Batting_Team, Bowling_Team, OverNum, Ball));''')
    cur.execute('''Create Table Team_Matches(TeamName varchar(20) Not null references Team(TeamName), Match_ID int not null references Matches(Match_ID),primary key (TeamName, Match_ID));''')
    cur.execute('''Create table Team_Deliveries(TeamName varchar(20) Not null references Team(TeamName), Match_ID int not null references Matches(Match_ID), inning int not null references Deliveries(inning), Batting_team Varchar(50) not null references Deliveries(Batting_team), Bowling_team varchar(50) not null references Deliveries(Bowling_team), Overnum int not null references Deliveries(overnum), Ball int not null references Deliveries(ball), Primary Key (TeamName, Match_ID, inning, Batting_team, Bowling_team, Overnum, ball));''')

#Create table Team_Deliveries(
#TeamN varchar(20) Not null references Team(TeamN),
#Match_ID int not null references Matches(Match_ID),
#inning int not null references Deliveries(inning),
#Batting_team Varchar(50) not null references Deliveries(Batting_team),
#Bowling_team varchar(50) not null references Deliveries(Bowling_team),
#Overnum int not null references Deliveries(overnum),
#Ball int not null references Deliveries(ball),
#Primary Key (TeamN, Match_ID, inning, Batting_team, Bowling_team, Overnum, ball));


#Create Table Team_Matches(
#TeamN varchar(20) Not null references Team(TeamN),
#Match_ID int not null references Matches(Match_ID),
#primary key (TeamN, Match_ID));

#Create Table Player_Deliveries (
#Team Varchar(20) Not Null references Player(Name),
#DOB date REFERENCES Player(DOB),
#Match_ID int Not Null REFERENCES Deliveries(Match_ID),
#inning int REFERENCES Deliveries(inning),
#batting_team varchar(50) REFERENCES Deliveries(batting_team),
#bowling_team varchar(50) REFERENCES Deliveries(bowling_team),
#overnum int REFERENCES Deliveries(overnum),
#ball int REFERENCES Deliveries(ball),
#Primary Key (Team, DOB, Match_ID, inning, Batting_Team, Bowling_Team, OverNum, Ball));

#Create Table Deliveries_Matches(
#Match_ID int Not null references Matches(Match_ID),
#inning int not null references Deliveries(inning),
#Batting_team Varchar(50) not null references Deliveries(Batting_team),
#Bowling_team varchar(50) not null references Deliveries(Bowling_team),
#Overnum int not null references Deliveries(overnum),
#Ball int not null references Deliveries(ball),
#Primary Key (Match_ID, inning, Batting_team, Bowling_team, Overnum, ball));

#'''CREATE TABLE Player  (
#Name           VARCHAR(50) NOT NULL PRIMARY KEY,
#DOB            Date        NOT NULL PRIMARY KEY,
#Batting_Hand   VARCHAR(15),
#country        VARCHAR(30),
#Bowling_Skill  varchar(25),
#team           varchar(50)
#FOREIGN key (team) references Team(TeamName) );''')

#CREATE TABLE Deliveries  (
#Match_ID          Int NOT NULL,
#inning            Int,
#Batting_Team      VARCHAR(50),
#Bowling_Team      VARCHAR(50),
#OverNum           Int,
#Ball              Int,
#Batsman           Varchar(50),
#Non_Striker       Varchar(50),
#Bowler            Varchar(50),
#Primary Key(Match_ID, inning, Batting_Team, Bowling_Team, OverNum, Ball)
#);

#CREATE TABLE Matches(
#Match_ID INT NOT NULL PRIMARY KEY auto_increment,
#Season                  Varchar(50),
#city                    Varchar(50),
#Date                    DATE,
#Team1                   Varchar(50),
#Team2                   Varchar(50),
#TossWnner               Varchar(50),
#TossDescision           Varchar(5),
#Result                  Varchar(8)
#)
    # Create Join Tables


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
            Team = line.__getitem__(5)
            cur.execute('INSERT IGNORE INTO Player VALUES (%s,%s,%s,%s,%s,%s)',
            (PlayerName, DOB, Batting_Hand, Bowling_Skill, Country,Team))
            # print(ID_num,sample_ID,primary,secondary,additional_info)







    with open("data/teamwise_home_and_away.csv", 'r') as r1:
        # skips first line the headers
        next(r1)
        for line in r1:
            line = line.split(',')
            TeamName = line.__getitem__(0)
            home_wins = int(line.__getitem__(1))
            home_wins = str(home_wins)
            away_wins = int(line.__getitem__(2))
            away_wins = str(away_wins)
            home_matches = int(line.__getitem__(3))
            home_matches = str(home_wins)
            away_matches = int(line.__getitem__(4))
            away_matches = str(away_matches)


            cur.execute(
                'INSERT IGNORE INTO Team VALUES (%s,%s,%s,%s,%s)',
                (TeamName, home_wins, away_wins, home_wins,away_wins))
            # print(ID_num,sample_ID,primary,secondary,additional_info)



    # insertions for Yeast-gene and Localization table and Yeast-gene&localization join table
    """
    with open("data/deliveries.csv", 'r') as r1:
        # skips first line the headers
        next(r1)
        for line in r1:
            line = line.split(',')
            Match_ID = str(int(line.__getitem__(0)))
            inning = str(int(line.__getitem__(1)))
            Batting_team = line.__getitem__(2)
            Bowling_team = line.__getitem__(3)
            over = int(line.__getitem__(4))
            over = str(over)
            Ball = int(line.__getitem__(5))
            Ball = str(Ball)
            batsman = line.__getitem__(6)
            non_striker = line.__getitem__(7)
            bowler = line.__getitem__(8)
            cur.execute('INSERT IGNORE INTO Deliveries VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)',(Match_ID, inning, Batting_team, Bowling_team, over, Ball, batsman, non_striker, bowler))
            #cur.execute(
            #    'INSERT IGNORE INTO Localization(Localization_ID,Biological_Process_Loc,Cellular_Component_Loc,Molecular_Function) VALUES (%s,%s,%s,%s)',
            #    (ID_num, bp, cc, mf))
            #cur.execute('INSERT IGNORE INTO YeastGene_Localization(Gene_ID,Localization_ID) VALUES (%s,%s)', (gene, ID_num))
    # insertion for SC expression table
    """

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
            cur.execute('INSERT IGNORE INTO Matches VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)',(ID_num, season, city, date, team1,team2,toss_winner,toss_decision,result));

    with open("data/Team_Matches.csv", 'r') as r1:
        # skips first line the headers
        next(r1)
        for line in r1:
            line = line.split(',')
            Match_ID = line.__getitem__(0)
            Match_ID = str(Match_ID)
            TeamName = line.__getitem__(1)
            TeamName = TeamName[1:len(TeamName)- 1]
            cur.execute(
                'INSERT IGNORE INTO Team_Matches VALUES (%s,%s)',
                (TeamName, Match_ID))

    with open("data/Team_Deliveries.csv", 'r') as r1:
        next(r1)
        for line in r1:
            line = line.split(',')
            TeamName = line.__getitem__(0)
            TeamName = TeamName[1:len(TeamName)- 1]
            match_id = line.__getitem__(1)
            match_id = str(match_id)
            inning = line.__getitem__(2)
            inning = str(inning)
            batting_team = line.__getitem__(3)
            batting_team = batting_team[1:len(batting_team)- 1]
            bowler = line.__getitem__(4)
            bowler = bowler[1:len(bowler)- 1]
            over = line.__getitem__(5)
            over = str(over)
            ball = line.__getitem__(6)
            ball = str(ball)
            cur.execute('INSERT IGNORE INTO Team_Deliveries VALUES (%s,%s,%s,%s,%s,%s,%s)',(TeamName, match_id, inning, batting_team, bowler,over,ball));

   

cnx = make_connection()
cur = cnx.cursor()
setup_dp(cur)
insert_data(cur)
cur.close()
cnx.commit()
cnx.close()
