# GenerateTestDataMP


First, create a new "mpload_<tablename>" class, by copying an existing one.

cockroach_manager.py should not need to change.  this is used to connect to the database.

Change load_crdb.py to load and call the new "mpload_<tablename>" class.




## For mpload_asset_forerst/mpload_asset_forrest_v2
Forrest
[{level: 0, level_0_id:"<this id>", level_ids:("<just this 1 id>"), parents:[]}, 
    {level:1, level_0_id:"id", level_ids:("id1", "id2", "id3"), parents:[{level:0, id:"level 0 parent"}]}, 
        {level: 2, level_0_id: "id", level_ids:("id1", "id2", "id3","id4"), parents:[{level: 0, id: level 0 parent}, {level:1, id: "level 1 parent"}]},
            {level: 3, level_0_id: "id", level_ids:("id1", "id2"), parents:[{level: 0, id: "level 0 parent"}, {level:1, id: "level 1 parent}, {level: 2, id: "level 2 parent"}]},
            {level: 3, level_0_id: "id", level_ids:("id1", "id2")},
            {level: 3, level_0_id: "id", level_ids:("id1", "id2")},
            {level: 3, level_0_id: "id", level_ids:("id1", "id2")},
        {level: 2, level_0_id: "id", level_ids:("id1", "id2"), parents:[{level: 0, id: level 0 parent}, {level:1, id: "level 1 parent"}]}},
            {level: 3, level_0_id: "id", level_ids:("id1", "id2")},
            {level: 3, level_0_id: "id", level_ids:("id1", "id2")},
        {level: 2, level_0_id: "id", level_ids:("id1", "id2", "id3","id4", "id5")},
            {level: 3, level_0_id: "id", level_ids:("id1", "id2")},
            {level: 3, level_0_id: "id", level_ids:("id1", "id2")},
            {level: 3, level_0_id: "id", level_ids:("id1", "id2")},
            {level: 3, level_0_id: "id", level_ids:("id1", "id2")},
            {level: 3, level_0_id: "id", level_ids:("id1", "id2")},
    {level:1, level_0_id:"id", level_ids:("id1", "id2", "id3"), parents:[{level:0, id:"level 0 parent"}]}, 
        {level: 2, level_0_id: "id", level_ids:("id1", "id2", "id3","id4"), parents:[{level: 0, id: level 0 parent}, {level:1, id: "level 1 parent"}]}},
            {level: 3, level_0_id: "id", level_ids:("id1", "id2")},
            {level: 3, level_0_id: "id", level_ids:("id1", "id2")},
            {level: 3, level_0_id: "id", level_ids:("id1", "id2")},
            {level: 3, level_0_id: "id", level_ids:("id1", "id2")},
        {level: 2, level_0_id: "id", level_ids:("id1", "id2"), parents:[{level: 0, id: level 0 parent}, {level:1, id: "level 1 parent"}]}},
            {level: 3, level_0_id: "id", level_ids:("id1", "id2")},
            {level: 3, level_0_id: "id", level_ids:("id1", "id2")},
        {level: 2, level_0_id: "id", level_ids:("id1", "id2", "id3","id4", "id5"), parents:[{level: 0, id: level 0 parent}, {level:1, id: "level 1 parent"}]}}
]

Closure
Add all parents for each child.  There will be existing records at the higher levels.
At level 1, I just need to add the level 1.
At level 2, I need to add level 2 to level 1 and the level 2 to level 0
At level 3, I need to add level 3 to level 2, the level 3 to level 1 and level 3 to level 0

Layout:  Elements of the list:
1.  The level of the element
2.  The UUID of the element (PK)
3.  A list of lists where each element represents a parent.  The elements of the list:
    1.  The level of the parent
    2.  The UUID of the parent


x='''[
    [0, "A-A-A", "A-A-A", "ANAME",  []],
    [1, "B-B-B", "A-A-A", "BNAME",  [[0, "A-A-A", "ANAME"]]],
    [1, "C-C-C", "A-A-A", "CNAME",  [[0, "A-A-A", "ANAME"]]],
    [2, "D-D-D", "A-A-A", "DNAME",  [[0, "A-A-A", "ANAME"], [1, "B-B-B", "BNAME"]]],
    [2, "E-E-E", "A-A-A", "ENAME",  [[0, "A-A-A", "ANAME"], [1, "B-B-B", "BNAME"]]],
    [2, "F-F-F", "A-A-A", "FNAME",  [[0, "A-A-A", "ANAME"], [1, "B-B-B", "BNAME"]]],
    [2, "G-G-G", "A-A-A", "GNAME",  [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"]]],
    [2, "H-H-H", "A-A-A", "HNAME",  [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"]]],
    [2, "I-I-I", "A-A-A", "INAME",  [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"]]],
    [3, "J-J-J", "A-A-A", "JNAME",  [[0, "A-A-A", "ANAME"], [1, "B-B-B", "BNAME"], [2, "D-D-D", "DNAME"]]],
    [3, "K-K-K", "A-A-A", "KNAME",  [[0, "A-A-A", "ANAME"], [1, "B-B-B", "BNAME"], [2, "D-D-D", "DNAME"]]],
    [3, "L-L-L", "A-A-A", "LNAME",  [[0, "A-A-A", "ANAME"], [1, "B-B-B", "BNAME"], [2, "D-D-D", "DNAME"]]],
    [3, "M-M-M", "A-A-A", "MNAME",  [[0, "A-A-A", "ANAME"], [1, "B-B-B", "BNAME"], [2, "D-D-D", "DNAME"]]],
    [3, "N-N-N", "A-A-A", "NNAME",  [[0, "A-A-A", "ANAME"], [1, "B-B-B", "BNAME"], [2, "E-E-E", "ENAME"]]],
    [3, "O-O-O", "A-A-A", "ONAME",  [[0, "A-A-A", "ANAME"], [1, "B-B-B", "BNAME"], [2, "E-E-E", "ENAME"]]],
    [3, "P-P-P", "A-A-A", "PNAME",  [[0, "A-A-A", "ANAME"], [1, "B-B-B", "BNAME"], [2, "F-F-F", "FNAME"]]],
    [3, "Q-Q-Q", "A-A-A", "QNAME",  [[0, "A-A-A", "ANAME"], [1, "B-B-B", "BNAME"], [2, "F-F-F", "FNAME"]]],
    [3, "R-R-R", "A-A-A", "RNAME",  [[0, "A-A-A", "ANAME"], [1, "B-B-B", "BNAME"], [2, "F-F-F", "FNAME"]]],
    [3, "S-S-S", "A-A-A", "SNAME",  [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "G-G-G", "GNAME"]]],
    [3, "T-T-T", "A-A-A", "TNAME",  [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "G-G-G", "GNAME"]]],
    [3, "U-U-U", "A-A-A", "UNAME",  [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "G-G-G", "GNAME"]]],
    [3, "V-V-V", "A-A-A", "VNAME",  [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "G-G-G", "GNAME"]]],
    [3, "W-W-W", "A-A-A", "WNAME",  [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "H-H-H", "HNAME"]]],
    [3, "X-X-X", "A-A-A", "XNAME",  [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "H-H-H", "HNAME"]]],
    [4, "AA",    "A-A-A", "AANAME", [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "G-G-G", "GNAME"], [3, "S-S-S", "SNAME"]]]    
    [4, "BB",    "A-A-A", "BBNAME", [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "G-G-G", "GNAME"], [3, "S-S-S", "SNAME"]]]    
    [4, "CC",    "A-A-A", "CCNAME", [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "G-G-G", "GNAME"], [3, "S-S-S", "SNAME"]]]    
    [4, "DD",    "A-A-A", "DDNAME", [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "H-H-H", "HNAME"], [3, "W-W-W", "WNAME"]]]    
    [4, "EE",    "A-A-A", "EENAME", [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "H-H-H", "HNAME"], [3, "W-W-W", "WNAME"]]]    
    [4, "FF",    "A-A-A", "FFNAME", [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "H-H-H", "HNAME"], [3, "W-W-W", "WNAME"]]]    
    [5, "GG",    "A-A-A", "GGNAME", [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "G-G-G", "GNAME"], [3, "S-S-S", "SNAME"], [4, "CC", "CNAME"]]]    
    [5, "HH",    "A-A-A", "HHNAME", [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "G-G-G", "GNAME"], [3, "S-S-S", "SNAME"], [4, "CC", "CNAME"]]]    
    [5, "II",    "A-A-A", "IINAME", [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "G-G-G", "GNAME"], [3, "S-S-S", "SNAME"], [4, "CC", "CNAME"]]]    
]'''


import uuid
import json
import faker
fake=faker.Faker()

A=str(uuid.uuid4())
B=str(uuid.uuid4())
C=str(uuid.uuid4())
D=str(uuid.uuid4())
E=str(uuid.uuid4())
F=str(uuid.uuid4())
G=str(uuid.uuid4())
H=str(uuid.uuid4())
I=str(uuid.uuid4())
J=str(uuid.uuid4())
K=str(uuid.uuid4())
L=str(uuid.uuid4())
M=str(uuid.uuid4())
N=str(uuid.uuid4())
O=str(uuid.uuid4())
P=str(uuid.uuid4())
Q=str(uuid.uuid4())
R=str(uuid.uuid4())
S=str(uuid.uuid4())
T=str(uuid.uuid4())
U=str(uuid.uuid4())
V=str(uuid.uuid4())
W=str(uuid.uuid4())
X=str(uuid.uuid4())
AA=str(uuid.uuid4())
BB=str(uuid.uuid4())
CC=str(uuid.uuid4())
DD=str(uuid.uuid4())
EE=str(uuid.uuid4())
FF=str(uuid.uuid4())
GG=str(uuid.uuid4())
HH=str(uuid.uuid4())
II=str(uuid.uuid4())

ANAME=str(uuid.uuid4())
BNAME=str(uuid.uuid4())
CNAME=str(uuid.uuid4())
DNAME=str(uuid.uuid4())
ENAME=str(uuid.uuid4())
FNAME=str(uuid.uuid4())
GNAME=str(uuid.uuid4())
HNAME=str(uuid.uuid4())
INAME=str(uuid.uuid4())
JNAME=str(uuid.uuid4())
KNAME=str(uuid.uuid4())
LNAME=str(uuid.uuid4())
MNAME=str(uuid.uuid4())
NNAME=str(uuid.uuid4())
ONAME=str(uuid.uuid4())
PNAME=str(uuid.uuid4())
QNAME=str(uuid.uuid4())
RNAME=str(uuid.uuid4())
SNAME=str(uuid.uuid4())
TNAME=str(uuid.uuid4())
UNAME=str(uuid.uuid4())
VNAME=str(uuid.uuid4())
WNAME=str(uuid.uuid4())
XNAME=str(uuid.uuid4())
AANAME=str(uuid.uuid4())
BBNAME=str(uuid.uuid4())
CCNAME=str(uuid.uuid4())
DDNAME=str(uuid.uuid4())
EENAME=str(uuid.uuid4())
FFNAME=str(uuid.uuid4())
GGNAME=str(uuid.uuid4())
HHNAME=str(uuid.uuid4())
IINAME=str(uuid.uuid4())

x='[[0, "'+A+'", "'+A+'", []],[1, "'+B+'", "'+A+'", [[0, "'+A+'"]]],[1, "'+C+'", "'+A+'", [[0, "'+A+'"]]],[2, "'+D+'", "'+A+'", [[0, "'+A+'"], [1, "'+B+'"]]],[2, "'+E+'", "'+A+'", [[0, "'+A+'"], [1, "'+B+'"]]],[2, "'+F+'", "'+A+'", [[0, "'+A+'"], [1, "'+B+'"]]],[2, "'+G+'", "'+A+'", [[0, "'+A+'"], [1, "'+C+'"]]],[2, "'+H+'", "'+A+'", [[0, "'+A+'"], [1, "'+C+'"]]],[2, "'+I+'", "'+A+'", [[0, "'+A+'"], [1, "'+C+'"]]],[3, "'+J+'", "'+A+'", [[0, "'+A+'"], [1, "'+B+'"], [2, "'+C+'"]]],[3, "'+K+'", "'+A+'", [[0, "'+A+'"], [1, "'+B+'"], [2, "'+C+'"]]],[3, "'+L+'", "'+A+'", [[0, "'+A+'"], [1, "'+B+'"], [2, "'+C+'"]]],[3, "'+M+'", "'+A+'", [[0, "'+A+'"], [1, "'+B+'"], [2, "'+C+'"]]],[3, "'+N+'", "'+A+'", [[0, "'+A+'"], [1, "'+B+'"], [2, "'+E+'"]]],[3, "'+O+'", "'+A+'", [[0, "'+A+'"], [1, "'+B+'"], [2, "'+E+'"]]],[3, "'+P+'", "'+A+'", [[0, "'+A+'"], [1, "'+B+'"], [2, "'+F+'"]]],[3, "'+Q+'", "'+A+'", [[0, "'+A+'"], [1, "'+B+'"], [2, "'+F+'"]]],[3, "'+R+'", "'+A+'", [[0, "'+A+'"], [1, "'+B+'"], [2, "'+F+'"]]],[3, "'+S+'", "'+A+'", [[0, "'+A+'"], [1, "'+B+'"], [2, "'+G+'"]]],[3, "'+T+'", "'+A+'", [[0, "'+A+'"], [1, "'+B+'"], [2, "'+G+'"]]],[3, "'+U+'", "'+A+'", [[0, "'+A+'"], [1, "'+B+'"], [2, "'+G+'"]]],[3, "'+V+'", "'+A+'", [[0, "'+A+'"], [1, "'+B+'"], [2, "'+G+'"]]],[3, "'+W+'", "'+A+'", [[0, "'+A+'"], [1, "'+B+'"], [2, "'+H+'"]]],[3, "'+X+'", "'+A+'", [[0, "'+A+'"], [1, "'+B+'"], [2, "'+H+'"]]], [4, "'+AA+'",    "'+A+'", [[0, "'+A+'"], [1, "'+C+'"], [2, "'+G+'"], [3, "'+S+'"]]],[4, "'+BB+'",    "'+A+'", [[0, "'+A+'"], [1, "'+C+'"], [2, "'+G+'"], [3, "'+S+'"]]],[4, "'+CC+'",    "'+A+'", [[0, "'+A+'"], [1, "'+C+'"], [2, "'+G+'"], [3, "'+S+'"]]],[4, "'+DD+'",    "'+A+'", [[0, "'+A+'"], [1, "'+C+'"], [2, "'+H+'"], [3, "'+W+'"]]],[4, "'+EE+'",    "'+A+'", [[0, "'+A+'"], [1, "'+C+'"], [2, "'+H+'"], [3, "'+W+'"]]],[4, "'+FF+'",    "'+A+'", [[0, "'+A+'"], [1, "'+C+'"], [2, "'+H+'"], [3, "'+W+'"]]],[5, "'+GG+'",    "'+A+'", [[0, "'+A+'"], [1, "'+C+'"], [2, "'+G+'"], [3, "'+S+'"], [4, "'+CC+'"]]],[5, "'+HH+'",    "'+A+'", [[0, "'+A+'"], [1, "'+C+'"], [2, "'+G+'"], [3, "'+S+'"], [4, "'+CC+'"]]],[5, "'+II+'",    "'+A+'", [[0, "'+A+'"], [1, "'+C+'"], [2, "'+G+'"], [3, "'+S+'"], [4, "'+CC+'"]]]]'  

y=json.loads(x)

x='[[0, "'+A+'", "'+A+'", "'+ANAME+'",  []],[1, "'+B+'", "'+A+'", "'+BNAME+'",  [[0, "'+A+'", "'+ANAME+'"]]],[1, "'+C+'", "'+A+'", "'+CNAME+'",  [[0, "'+A+'", "'+ANAME+'"]]],[2, "'+D+'", "'+A+'", "'+DNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+B+'", "'+BNAME+'"]]],[2, "'+E+'", "'+A+'", "'+ENAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+B+'", "'+BNAME+'"]]],[2, "'+F+'", "'+A+'", "'+FNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+B+'", "'+BNAME+'"]]],[2, "'+G+'", "'+A+'", "'+GNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"]]],[2, "'+H+'", "'+A+'", "'+HNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"]]],[2, "'+I+'", "'+A+'", "'+INAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"]]],[3, "'+J+'", "'+A+'", "'+JNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+B+'", "'+BNAME+'"], [2, "'+D+'", "'+DNAME+'"]]],[3, "'+K+'", "'+A+'", "'+KNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+B+'", "'+BNAME+'"], [2, "'+D+'", "'+DNAME+'"]]],[3, "'+L+'", "'+A+'", "'+LNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+B+'", "'+BNAME+'"], [2, "'+D+'", "'+DNAME+'"]]],[3, "'+M+'", "'+A+'", "'+MNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+B+'", "'+BNAME+'"], [2, "'+D+'", "'+DNAME+'"]]],[3, "'+N+'", "'+A+'", "'+NNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+B+'", "'+BNAME+'"], [2, "'+E+'", "'+ENAME+'"]]],[3, "'+O+'", "'+A+'", "'+ONAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+B+'", "'+BNAME+'"], [2, "'+E+'", "'+ENAME+'"]]],[3, "'+P+'", "'+A+'", "'+PNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+B+'", "'+BNAME+'"], [2, "'+F+'", "'+FNAME+'"]]],[3, "'+Q+'", "'+A+'", "'+QNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+B+'", "'+BNAME+'"], [2, "'+F+'", "'+FNAME+'"]]],[3, "'+R+'", "'+A+'", "'+RNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+B+'", "'+BNAME+'"], [2, "'+F+'", "'+FNAME+'"]]],[3, "'+S+'", "'+A+'", "'+SNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+G+'", "'+GNAME+'"]]],[3, "'+T+'", "'+A+'", "'+TNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+G+'", "'+GNAME+'"]]],[3, "'+U+'", "'+A+'", "'+UNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+G+'", "'+GNAME+'"]]],[3, "'+V+'", "'+A+'", "'+VNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+G+'", "'+GNAME+'"]]],[3, "'+W+'", "'+A+'", "'+WNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+H+'", "'+HNAME+'"]]],[3, "'+X+'", "'+A+'", "'+XNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+H+'", "'+HNAME+'"]]],[4, "'+AA+'","'+A+'", "'+AANAME+'", [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+G+'", "'+GNAME+'"], [3, "'+S+'", "'+SNAME+'"]]],    [4, "'+BB+'","'+A+'", "'+BBNAME+'", [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+G+'", "'+GNAME+'"], [3, "'+S+'", "'+SNAME+'"]]],    [4, "'+CC+'","'+A+'", "'+CCNAME+'", [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+G+'", "'+GNAME+'"], [3, "'+S+'", "'+SNAME+'"]]],    [4, "'+DD+'","'+A+'", "'+DDNAME+'", [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+H+'", "'+HNAME+'"], [3, "'+W+'", "'+WNAME+'"]]],    [4, "'+EE+'","'+A+'", "'+EENAME+'", [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+H+'", "'+HNAME+'"], [3, "'+W+'", "'+WNAME+'"]]],    [4, "'+FF+'","'+A+'", "'+FFNAME+'", [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+H+'", "'+HNAME+'"], [3, "'+W+'", "'+WNAME+'"]]],    [5, "'+GG+'","'+A+'", "'+GGNAME+'", [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+G+'", "'+GNAME+'"], [3, "'+S+'", "'+SNAME+'"], [4, "'+CC+'", "'+CNAME+'"]]],    [5, "'+HH+'","'+A+'", "'+HHNAME+'", [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+G+'", "'+GNAME+'"], [3, "'+S+'", "'+SNAME+'"], [4, "'+CC+'", "'+CNAME+'"]]],    [5, "'+II+'","'+A+'", "'+IINAME+'", [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+G+'", "'+GNAME+'"], [3, "'+S+'", "'+SNAME+'"], [4, "'+CC+'", "'+CNAME+'"]]]]'







