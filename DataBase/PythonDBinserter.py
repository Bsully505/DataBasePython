import pymysql as ps
import matplotlib.pyplot as plt
import matplotlib.axis as ax
import mplcursors

#AUTHORS BRYAN SULLIVAN and HENOK KETSELA

# Connecting to Database
def make_connection():
    return ps.connect(host='bryandbruby.cucbdvc0bgxn.us-east-1.rds.amazonaws.com', user='admin',
                      passwd='admin123',
                      port=3306, autocommit=True)
def setup_dp(cur):

    # Set up db
    cur.execute('DROP DATABASE IF EXISTS IPL_DATA_SET');
    cur.execute('CREATE DATABASE IPL_DATA_SET');
    cur.execute('USE IPL_DATA_SET');

    # Drop Existing Tables

    cur.execute('DROP TABLE IF EXISTS Player');
    cur.execute('DROP TABLE IF EXISTS Deliveries');
    cur.execute('DROP TABLE IF EXISTS Team');
    cur.execute('DROP TABLE IF EXISTS Player_Deliveries');
    cur.execute('DROP TABLE IF EXISTS Team_Deliveries');
    cur.execute('DROP TABLE IF EXISTS Deliveries_Matches');
    cur.execute('DROP TABLE IF EXISTS Team_Matches');
    cur.execute('DROP TABLE IF EXISTS Matches');
    cur.execute('DROP TABLE IF EXISTS Cities');

    # Create Tables
    cur.execute('''CREATE TABLE Team(TeamName Varchar(50) NOT NULL PRIMARY KEY, HomeWins Int,AwayWins Int, HomeMatches Int, AwayMatches Int);''')
    cur.execute('''CREATE TABLE Player(Name VARCHAR(50) NOT NULL PRIMARY KEY,DOB VarChar(50) NOT NULL,Batting_Hand VARCHAR(15),Bowling_Skill varchar(25) not null, country VARCHAR(30),team varchar(50) references Team(TeamName) );''')
    cur.execute('''CREATE TABLE Deliveries(Match_ID Int NOT NULL, inning Int, Batting_Team VARCHAR(50), Bowling_Team VARCHAR(50), OverNum Int, Ball Int, Batsman Varchar(50), Non_Striker Varchar(50), Bowler Varchar(50), Primary Key(Match_ID, inning, Batting_Team, Bowling_Team, OverNum, Ball));''')
    cur.execute('''create Table Deliveries_Matches(Match_ID int Not Null references Matches(Match_ID), inning int not null references Deliveries(inning), Batting_team Varchar(50) not null references Deliveries(Batting_team), Bowling_team varchar(50) not null references Deliveries(Bowling_team), Overnum int not null references Deliveries(overnum), Ball int not null references Deliveries(ball), Primary Key (Match_ID, inning, Batting_team, Bowling_team, Overnum, ball));''')
    cur.execute('''Create Table Player_Deliveries (Team Varchar(50) Not Null references Player(Name), DOB Varchar(20) REFERENCES Player(DOB), Match_ID int Not Null REFERENCES Deliveries(Match_ID), inning int REFERENCES Deliveries(inning), batting_team varchar(50) REFERENCES Deliveries(batting_team), bowling_team varchar(50) REFERENCES Deliveries(bowling_team), overnum int REFERENCES Deliveries(overnum), ball int REFERENCES Deliveries(ball), Primary Key (Team, DOB, Match_ID, inning, Batting_Team, Bowling_Team, OverNum, Ball));''')
    cur.execute('''Create Table Team_Matches(TeamName varchar(50) Not null references Team(TeamName), Match_ID int not null references Matches(Match_ID),primary key (TeamName, Match_ID));''')
    cur.execute('''Create table Team_Deliveries(TeamName varchar(50) Not null references Team(TeamName), Match_ID int not null references Matches(Match_ID), inning int not null references Deliveries(inning), Batting_team Varchar(50) not null references Deliveries(Batting_team), Bowling_team varchar(50) not null references Deliveries(Bowling_team), Overnum int not null references Deliveries(overnum), Ball int not null references Deliveries(ball), Primary Key (TeamName, Match_ID, inning, Batting_team, Bowling_team, Overnum, ball));''')
    cur.execute('''Create Table Matches (MatchID int NOT Null auto_increment Primary key, Season Varchar(50), CityID int references Cities(CityID), Date Varchar(20), Team1 Varchar(50), Team2 Varchar(50), Tosswinner Varchar(50), Tossdecision Varchar(50), Result VarChar(8));''')
    cur.execute('''Create table Cities(CityID int Not null Auto_increment, CityName varchar(50) not null references Matches(City), Primary Key(CityID));''')
    cur.execute('''Create table DistinctCities(DistinctCityName varchar(50) not null, Primary Key(DistinctCityName));''')



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
            home_matches = str(home_matches)
            away_matches = int(line.__getitem__(4))
            away_matches = str(away_matches)
            cur.execute(
                'INSERT IGNORE INTO Team VALUES (%s,%s,%s,%s,%s)',
                (TeamName, home_wins, away_wins, home_matches,away_matches))


    # insertions for deliveries join table
    with open("data/deliveriesSmall.csv", 'r') as r1:
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

            # insertion for Matches expression table
    with open("data/matches.csv", 'r') as r1:
        next(r1)
        for line in r1:
            line = line.split(',')
            city = line.__getitem__(2);
            cur.execute('Insert Ignore into DistinctCities Values(%s)',(city));


    cur.execute('Select * from DistinctCities');
    DistictCityResults = cur.fetchall();
    for i in range(0,len(DistictCityResults)):
        cur.execute('Insert Ignore into Cities(CityName) Values(%s)',(DistictCityResults[i][0]));



    with open("data/matches.csv", 'r') as r1:
        next(r1)
        for line in r1:
            line = line.split(',')
            ID_num = int(line.__getitem__(0))
            ID_num = str(ID_num)
            season = line.__getitem__(1)
            cur.execute('''select CityID from Cities where CityName = (%s)''',(line.__getitem__(2)));
            cityid = cur.fetchall()[0]
            date = line.__getitem__(3)
            team1 = line.__getitem__(4)
            team2 = line.__getitem__(5)
            toss_winner = line.__getitem__(6)
            toss_decision = line.__getitem__(7)
            result = line.__getitem__(8)
            cur.execute('INSERT IGNORE INTO Matches VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)',(ID_num, season, cityid, date, team1,team2,toss_winner,toss_decision,result));


     # insertion for Team_Matches and Team_Deliveries and Player_Deliveries expression table
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

    with open("data/Player_Deliveries.csv", 'r') as r1:
         next(r1)
         for line in r1:
             line = line.split(',')
             Team = line.__getitem__(0)
             Team = Team[1:len(Team)- 1]
             DOB = line.__getitem__(1)
             Match_ID = line.__getitem__(2)
             Match_ID = str(Match_ID)
             inning = line.__getitem__(3)
             inning = str(inning)
             Batting_Team = line.__getitem__(4)
             Batting_Team = Batting_Team[1:len(Batting_Team)- 1]
             Bowling_Team = line.__getitem__(5)
             Bowling_Team = Bowling_Team[1:len(Bowling_Team)- 1]
             over = line.__getitem__(6)
             over = str(over)
             Ball = line.__getitem__(7)
             Ball = str(Ball)
             cur.execute('INSERT IGNORE INTO Player_Deliveries VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',(Team, DOB, Match_ID, inning, Batting_Team, Bowling_Team, over, Ball));

     # insertion for Matches_Deliveries expression table
    with open("data/Matches_Deliveries.csv", 'r') as r1:
            next(r1)
            for line in r1:
                line = line.split(',')
                MatchID = int(line.__getitem__(0))
                MatchID = str(MatchID)
                inning = line.__getitem__(1)
                inning = str(inning)
                batting_team = line.__getitem__(2)
                batting_team = batting_team[1:len(batting_team)- 1]
                bowling_team = line.__getitem__(3)
                bowling_team = bowling_team[1:len(bowling_team)- 1]
                over = line.__getitem__(4)
                over = str(over)
                ball = line.__getitem__(5)
                ball = str(ball)
                cur.execute('INSERT IGNORE INTO Deliveries_Matches VALUES (%s,%s,%s,%s,%s,%s)',( MatchID, inning, batting_team, bowling_team,over,ball));





def createGraph(cur):
    fig = plt.figure();
    fig.subplots_adjust(wspace=0.5, hspace=0.70, left=0.07,right=0.98,top=0.905,bottom=0.129);#changes the dimensions
    fig.tight_layout();
    ax = plt.subplot(2,3,1)
    HomeWinandAwayWinGraph(cur, ax);#allowing to create two graphs using one select statement allows the program to run faster
    ax = plt.subplot(2,3,3);
    CreateDOBGraph(cur,ax);
    ax = plt.subplot(2,2,3);
    LocationOfMatchesGraph(cur,ax,fig);
    ax = plt.subplot(2,2,4);
    coinTossWinnerGraph(cur,ax)
    plt.show();


def CreateDOBGraph(cur,ax):
    cur.execute('use IPL_DATA_SET');
    cur.execute('select DOB from Player');
    QuerryResponse= cur.fetchall();
    count69thru72 = 0;
    count73thru75 = 0;
    count76thru78 = 0;
    count79thru81 = 0;
    count82thru84 = 0;
    count85thru86 = 0;
    count87thru89 = 0;
    count90thru92 = 0;
    count93thru95 = 0;
    count96thru98 = 0;
    for i in range(0,len(QuerryResponse)):
        if(QuerryResponse[i][0] != ''):
            PlayerYear = int( QuerryResponse[i][0].split('-')[2])
            if(PlayerYear<73):#this is a switch statement to determine how many people were born in each year
                count69thru72+=1;
            elif(PlayerYear<76):
                count73thru75+=1;
            elif(PlayerYear<79):
                count76thru78+=1;
            elif(PlayerYear<82):
                count79thru81+=1;
            elif(PlayerYear<85):
                count82thru84+=1;
            elif(PlayerYear<87):
                count85thru86+=1;
            elif(PlayerYear<90):
                count87thru89+=1;
            elif(PlayerYear<93):
                count90thru92+=1;
            elif(PlayerYear<96):
                count93thru95+=1;
            elif(PlayerYear<99):
                count96thru98+=1;

    dateRanges= ['69-72', '73-75','76-78','79-81','82-84','85-86','87-89','90-92','93-95','96-99']
    AmtDOBinAgeRange= [count69thru72,count73thru75,count76thru78,count79thru81,count82thru84,count85thru86,count87thru89,count90thru92,count93thru95,count96thru98]
    ax.bar(dateRanges,AmtDOBinAgeRange);
    ax.set_title('Birth Year');
    plt.ylabel("Amount of Players");
    plt.xlabel("Year (1900's)");
    plt.tick_params(axis='x', which='major', labelsize=7)



def AwayWinPercentGraph(teamname, AwayWin,ax):
    ax.plot(teamname,AwayWin)
    ax.set(title= 'Away Win %');
    plt.ylabel("Win %");
    plt.xlabel("Team");
    plt.ylim(0,100);
    ax.xaxis.set_label_coords(0.50, 0.075);
    plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    plt.tick_params(axis='x', which='major', labelsize=7)



def HomeWinPercentGraph(teamname, HomeWin,ax):
    ax.plot(teamname,HomeWin)
    ax.set(title='Home Win %');
    plt.ylabel("Win %");
    plt.xlabel("Team");
    plt.ylim(0,100);
    ax.xaxis.set_label_coords(0.50, 0.075);
    plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    plt.tick_params(axis='x', which='major', labelsize=7)


def LocationOfMatchesGraph(cur,ax,fig):
    cur.execute('use IPL_DATA_SET');
    cur.execute('Select city, count(*) from Matches group by city order by count(*)');#or should it be selet city, count(*) from Matches group by city
    QuerryResponse = cur.fetchall();
    Location=[]
    amount=[]
    otherAmt = 0;
    for i in range(0,len(QuerryResponse)):
        if(QuerryResponse[i][0] != ""):
            if(QuerryResponse[i][1] >=13):
                Location.insert(0,QuerryResponse[i][0]);
                amount.insert(0,QuerryResponse[i][1]);
            else:
                otherAmt += QuerryResponse[i][1]
    Location.insert(0,'Other');
    amount.insert(0,otherAmt);
    ax.set_title('Location of Matches',y = 1.28);
    ax.pie(amount, labels = Location,autopct='%1.1f%%',pctdistance =.8,radius = 1.57,labeldistance = 1, rotatelabels=True,startangle= 45);


def HomeWinandAwayWinGraph(cur,ax):#since the data of homewin and awaywins had a lot of similarities it made sense to put them in the same function
    cur.execute('use IPL_DATA_SET');
    cur.execute('Select TeamName, HomeWins, AwayWins,HomeMatches, AwayMatches From Team');
    QuerryResponse  = cur.fetchall();
    teamname= []
    HomeWin = []
    AwayWin = []
    for i in range(0, len(QuerryResponse)):
        teamname.insert(0, QuerryResponse[i][0]);
        HomeWin.insert(0,(QuerryResponse[i][1]/QuerryResponse[i][3])*100);
        AwayWin.insert(0,(QuerryResponse[i][2]/QuerryResponse[i][4])*100);

    HomeWinPercentGraph(teamname, HomeWin,ax);
    ax = plt.subplot(2,3,2);
    AwayWinPercentGraph(teamname,AwayWin,ax);

def coinTossWinnerGraph(cur, ax):
    cur.execute('use IPL_DATA_SET');
    cur.execute('Select Tosswinner, count(*) from Matches group by Tosswinner');
    QuerryResponse = cur.fetchall();
    Teams = []
    TeamWinCount = []
    for i in range(0,len(QuerryResponse)):
        Teams.insert(0,QuerryResponse[i][0]);
        TeamWinCount.insert(0,QuerryResponse[i][1]);
    ax.scatter(Teams,TeamWinCount);
    ax.set_title('Coin Toss Wins');
    plt.xlabel('Team')
    plt.ylabel('Number of Wins');
    ax.xaxis.set_label_coords(0.50, 0.075);
    plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    plt.tick_params(axis='x', which='major', labelsize=7)
    crs = mplcursors.cursor(ax,hover=True)

cnx = make_connection()
cur = cnx.cursor()
setup_dp(cur)
insert_data(cur)
createGraph(cur)
cur.close()
cnx.commit()
cnx.close()
