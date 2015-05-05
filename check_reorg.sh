#!/usr/bin/sh
var="dbmaintenance_reorg.log"

for i in `db2 list db directory|grep -ip Indirect|grep name|grep Database| awk '{print $4}'`

do

echo 'DBname = ' $i
DBname=$i

db2 connect to $i
chk=`db2 get health snapshot for all databases | grep -ip 'Attention' |  grep 'db.tb_reorg_req' `
if [[ $? -eq 0   ||  $? -eq 127 ]] ; then
   echo " REORG "
   db2 get recommendations for health indicator db.tb_reorg_req for db on $i | sed -n '/Rank: 1/,/Rank: 2/p' | grep -i ';' | grep -v 'CORAUDIT' | sed 's/;//' > reorg.temp
     while read line_by_line

      do

       rundbmaint.ksh "${line_by_line}" $DBname $var &
      done < reorg.temp
  fi
   echo  " Check Statistics"
  chks=`db2 get health snapshot for all databases | grep -ip 'Attention' |  grep 'db.tb_runstats_req' `
     if [[ $? -eq 0   ||  $? -eq 127 ]] ; then
       echo " UPDATE STATISTICS "
   db2 get recommendations for health indicator db.tb_runstats_req for db on $i | sed -n '/Rank: 1/,/Rank: 2/p' | grep -i ';' | sed 's/;//' > reorg.temp
     while read line_by_line

      do

      rundbmaint.ksh "${line_by_line}" $DBname $var &
      done < reorg.temp
fi
rm reorg.temp
db2 commit
db2rbind $i -l bind.log
done
#end
exit
