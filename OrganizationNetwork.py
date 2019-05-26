
import pymongo
import csv
import pprint
from bson.json_util import dumps
import mysql.connector
import csv
from mysql.connector.constants import ClientFlag


#MySQL host info
sqluser = input("Please enter the MYSQL user name:")
sqlpwd = input("please enter the MYSQL password:")
sqldb = input("Please enter the MYSQL database you want to run:")

mydb = mysql.connector.connect(
    host="localhost",
    user=sqluser,
    passwd=sqlpwd,
    database=sqldb,
    client_flags = [ClientFlag.LOCAL_FILES]
)

#Mongo host info
client = pymongo.MongoClient('localhost', 27017)

#Mongo create database
db = client.project1
users = db.user
organizations = db.organization

#Test to drop all tables if exist
mycursor = mydb.cursor(buffered=True)
#mycursor.execute("SHOW DATABASES")
mycursor.execute("drop table if exists user;")
mycursor.execute("drop table if exists skills;")
mycursor.execute("drop table if exists projects;")
mycursor.execute("drop table if exists organizations;")
mycursor.execute("drop table if exists interests;")
mycursor.execute("drop table if exists distance;")

# Ask users for csv file
disfile = input("Please Enter Distance file name:")
intfile = input("Please Enter Interest file name:")
orgfile = input("Please Enter Organization file name:")
profile = input("Please Enter Project file name:")
skillfile = input("Please Enter Skill file name:")
userfile = input("Please Enter User file name:")


#Mongo load csv file
with open(userfile) as csvfile:
    csvfile.readline() # skip the first line
    readCSV = csv.reader(csvfile, delimiter=',')
    for row in readCSV:
        filter = { "user_id": row[0]}
        update = { "$set":
                   {"user_id": row[0],
                    "first_name": row[1],
                    "last_name": row[2]}}
        users.update_one(filter, update, True)
		

with open(intfile) as csvfile:
    csvfile.readline() # skip the first line
    readCSV = csv.reader(csvfile, delimiter=',')
    for row in readCSV:
        filter = { "user_id": row[0]}
        update = { "$addToSet":
                      {"interests" :
                       {"interest" : row[1],
                        "interest_level": row[2]}}}
        users.update_one(filter, update)

with open(profile) as csvfile:
    csvfile.readline() # skip the first line
    readCSV = csv.reader(csvfile, delimiter=',')
    for row in readCSV:
        filter = { "user_id": row[0]}
        update = { "$addToSet":
                      {"projects" : row[1]}}
        users.update_one(filter, update)

with open(skillfile) as csvfile:
    csvfile.readline() # skip the first line
    readCSV = csv.reader(csvfile, delimiter=',')
    for row in readCSV:
        filter = { "user_id": row[0]}
        update = { "$addToSet":
                      {"skills" :
                       {"skill" : row[1],
                        "skill_level": row[2]}}}
        users.update_one(filter, update)

with open(orgfile) as csvfile:
    csvfile.readline() # skip the first line
    readCSV = csv.reader(csvfile, delimiter=',')
    for row in readCSV:
        filter = { "user_id": row[0]}
        update = { "$set":
                      {"organization" : row[1]}}
        users.update_one(filter, update)
        filter = { "organization": row[1]}
        update = {"$setOnInsert" :{"organization": row[1],
                        "organization_type": row[2]}}
        organizations.update_one(filter, update, True)

with open(disfile) as csvfile:
    csvfile.readline() # skip the first line
    readCSV = csv.reader(csvfile, delimiter=',')
    for row in readCSV:
        filter = { "organization": row[0]}
        update = { "$addToSet":
                      {"distances" :
                       {"organization" : row[1],
                        "distance": row[2]}}}
        organizations.update_one(filter, update)
        filter = { "organization": row[1]}
        update = { "$addToSet":
                      {"distances" :
                       {"organization" : row[0],
                        "distance": row[2]}}}
        organizations.update_one(filter, update)


# MySQL create tables
mycursor.execute("CREATE TABLE user (user_id INT, First_name VARCHAR(255), Last_name VARCHAR(255))")
mycursor.execute("CREATE TABLE skills (user_id INT, Skill VARCHAR(255), Skill_level INT)")
mycursor.execute("CREATE TABLE projects (user_id INT, Project VARCHAR(255))")
mycursor.execute("CREATE TABLE organizations (user_id INT, Organization VARCHAR(255), Organization_type VARCHAR(255))")
mycursor.execute("CREATE TABLE interests (user_id INT, Interest VARCHAR(255), Interest_level INT)")
mycursor.execute("CREATE TABLE distance (Organization1 VARCHAR(255), Organization2 VARCHAR(255), Distance FLOAT)")


# MySQL load csv file
mycursor.execute("load data local infile \'"+disfile+"\' into table distance fields terminated by \',\' ignore 1 lines;")
mycursor.execute("load data local infile \'"+intfile+"\' into table interests fields terminated by \',\' ignore 1 lines;")
#mycursor.execute("load data local infile \'"+organfile+"\' into table organizations fields terminated by \',\' ignore 1 lines;")
#mycursor.execute("load data local infile \'"+profile+"\' into table projects fields terminated by \',\' ignore 1 lines;")
mycursor.execute("load data local infile \'"+skillfile+"\' into table skills fields terminated by \',\' ignore 1 lines;")
mycursor.execute("load data local infile \'"+userfile+"\' into table user fields terminated by \',\' ignore 1 lines;")


with open(profile) as csvfile:
        csvfile.readline()
        readCSV = csv.reader(csvfile, delimiter=',')
        for row in readCSV:
            mycursor.execute('INSERT INTO projects(user_id,Project)' 'VALUES (%s,%s)',row)

with open(orgfile) as csvfile:
        csvfile.readline()
        readCSV = csv.reader(csvfile, delimiter=',')
        for row in readCSV:
            mycursor.execute('INSERT INTO organizations(user_id,Organization,Organization_type)' 'VALUES (%s,%s,%s)',row)     

mydb.commit()




while True:
    choice = input("Enter \n0 to search shared interest users within 10 miles,\n1 to search colleagues-of-colleagues,\n2 to search entity,\nq to quit:")
    if (choice == "2"):
        while True:
            name = input("Please Enter first name or last name to search (enter q for other options):")
            if (name == 'q'):
                break
            filter = { "$or": [{"first_name": name}, {"last_name": name}]}
            user = users.find_one(filter)
            if user != None:
                print("First Name: " + user["first_name"] + "\nLast Name: " + user["last_name"]
                      + "\nOrganization: " + user["organization"] + "\nUser ID:" + user["user_id"] +"\nProjects:" ,end = " ")
                print(*user["projects"],  sep=",")               
                y=[]
                for x in user["interests"]:
                    print(x)
                for x in user["skills"]:
                    print(x)
            else:
                print("Not Found!")
    elif (choice == "1"):
        while True:
            name = input("Please Enter first name or last name to search colleagues-of-colleagues (enter q for other options):")
            if (name == 'q'):
                break
            filter = { "$or": [{"first_name": name}, {"last_name": name}]}
            user = users.find_one(filter)            
            if user != None:
                filter = {"$and":[{"projects":{"$in": user["projects"]}},
                                  {"user_id":{"$ne":user["user_id"]}}]}
                coll = users.find(filter)
                pro = set()
                for x in coll:
                    pro.update(x["projects"])
                y=[]
                for x in user["interests"]:
                    y.append(x["interest"])
                filter = {"$and": [{"projects":{"$in": list(pro)}},
                                   {"user_id":{"$ne":user["user_id"]}},
                                   {"interests.interest": {"$in":y}}]}
                UsersSPj = users.find(filter)
                print("First name\t"+ "Last Name")
                if UsersSPj != None:
                    for n in UsersSPj:
                        print(n["first_name"] + "\t\t"+ n["last_name"])              
            else:
                print("Not Found!")
    elif (choice == "0"):
        while True:
            uid = input("Please Enter user id to search users with shared interest/skills within 10 miles (enter q for other options):")
            if (uid == 'q'):
                break
            elif (uid.isdigit()):
                sql ="""select u2.User_id, o2.organization, t1.i, t2.s
                from user u2
                left join(
                    select u2.User_id, GROUP_CONCAT(i1.interest) as i
                    from user u2
                    join interests i1 on i1.User_id =
                """ + uid +"""
                    join
                    (
                        select u1.User_id, GROUP_CONCAT(i1.interest) as interest
                        from user u1
                        left join interests i1 on i1.User_id = u1.User_id
                        group by u1.User_id
                    ) uAll on
                    uAll.User_id = u2.User_id and FIND_IN_SET(i1.interest, uAll.interest)
                    where u2.User_id !=
                  """ + uid +"""
                    group by u2.User_id
                ) t1 on t1.User_id = u2.User_id
                left join
                (
                    select u2.User_id, GROUP_CONCAT(s1.skill) as s
                    from user u2
                    join skills s1 on s1.User_id =
                 """ + uid +"""
                    join
                    (
                        select u1.User_id, GROUP_CONCAT(s1.skill) as skill
                        from user u1
                        left join skills s1 on s1.User_id = u1.User_id
                        group by u1.User_id
                    ) uAll on
                    uAll.User_id = u2.User_id and FIND_IN_SET(s1.skill, uAll.skill)
                    where u2.User_id !=
                 """ + uid +"""
                    group by u2.User_id
                ) t2 on t2.User_id = u2.User_id
                join organizations o1 on o1.User_id =
                 """ + uid +"""
                join organizations o2 on o2.User_id = u2.User_id
                where (t1.User_id is not null or t2.User_id is not null)
                and (o1.organization = o2.organization or exists
                (
                    select 1 from distance
                    where distance < 10 and ((organization1 = o1.organization and organization2 = o2.organization)
                    or (organization1 = o2.organization and organization2 = o1.organization))
                ))
                order by (IFNULL(LENGTH(t1.i) - LENGTH(REPLACE(t1.i, ',', '')), -1) + IFNULL(LENGTH(t2.s) - LENGTH(REPLACE(t2.s, ',', '')), -1)) desc;
                """
                mycursor.execute(sql)
                if mycursor.rowcount != 0:
                    myresult=mycursor.fetchall()
                    print("uid","user","interest","skill")
                    for x in myresult:
                        print(x)
                else:
                    print("NOT FOUND")
            else:
                print("need to be number")
    elif (choice == "q"):
        break

mycursor.close()
mydb.close()
client.close()
