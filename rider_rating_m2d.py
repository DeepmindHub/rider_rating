__author__="Anuran"
import pandas as pd
import mysql.connector as sqlcon
from pandas.io import sql
import time
import datetime as dt
import dbConfig_pw as db
import mailUtility as mu
import numpy as np
from pytz import reference

today=dt.datetime.now()
localtime=reference.LocalTimezone()
timezone=localtime.tzname(today)
if timezone=='IST':
  root_path = '/Users/anuran/Downloads/Work/sfrepo/anuran'
else:
  root_path = '/home/ubuntu/datareports/anuran'

date = dt.date.today()

def main():
  cnx = sqlcon.connect(user=db.USER, password=db.PWD,host=db.HOST, database=db.DATABASE)
  # cnx.start_transaction(isolation_level='READ COMMITTED')

  writer = pd.ExcelWriter(root_path+'/data/rider_rating_m2.xls')
  # BB_data=get_BB_data(cnx)
  # HL_data=get_HL_data(cnx)
  # BB_coeff=round(HL_data.eff[0]/BB_data.eff[0],2)
  default_sla=60
  BB_coeff=0.75
  print "BB_coeff",BB_coeff
  
  attendance_data=get_attendance_data(cnx)
  order_data=get_order_data(cnx,BB_coeff,default_sla)
  
  rider_data=attendance_data.merge(order_data,how='left',on=['rider_id','date','rider_name','cluster_id','cluster_name','city','role'])
  print "rider_data",rider_data.shape
  rider_data.fillna(0,inplace=True)
  rider_data['attendance_bonus']=rider_data.apply(get_att_bonus, axis=1)
  rider_data['order_per_hour']=rider_data['n_order_count']/rider_data['total_hours']
  rider_data['percent_delivery_sla']=rider_data['delivery_sla']/rider_data['n_order_count']
  rider_data.fillna(0,inplace=True)
  oph_coeff=20
  pds_coeff=50
  
  rider_data['score']=(oph_coeff*rider_data['order_per_hour']+pds_coeff*rider_data['percent_delivery_sla']+rider_data['attendance_bonus'])*(1-rider_data['tickets_count']/10)
  rider_data['weighted_score']=rider_data['score']
  l=0.3
  rider_data['count']=0
  

  query3='SELECT rider_id,cluster_id,city,date,attendance_status,total_hours,order_per_hour,n_order_count,ecom_orders,BB_orders,Pharmeasy_orders,other_orders,delivery_sla,percent_delivery_sla,tickets_count,score,count,weighted_score,rating FROM rider_rating_m2 WHERE DATE=curdate() - INTERVAL 2 day'
  previous_data=sql.read_sql(query3,cnx)

  # print rider_data
  # exit(-1)
  rider_data['rating']=0.0
  rider_data=rider_data[['rider_id','cluster_id','city','date','attendance_status','total_hours','order_per_hour','n_order_count','ecom_orders','BB_orders','Pharmeasy_orders','other_orders','delivery_sla','percent_delivery_sla','tickets_count','score','count','weighted_score','rating']]
  rider_data=pd.concat([rider_data,previous_data])
  rider_data=rider_data.sort_values(by=['rider_id','date'])
  # rider_data.to_csv(root_path+'/data/rider_data.csv',encoding='utf-8')
  
  rider_data=rider_data[['rider_id','cluster_id','city','date','attendance_status','total_hours','order_per_hour','n_order_count','ecom_orders','BB_orders','Pharmeasy_orders','other_orders','delivery_sla','percent_delivery_sla','tickets_count','score','count','weighted_score','rating']]
  rider_data.index=range(len(rider_data))

  for i in range(rider_data.shape[0]-1):
    # print rider_data.loc[i,'rider_id']
    # print rider_data.loc[i+1,'rider_id']
    
    if rider_data.loc[i+1,'rider_id']==rider_data.loc[i,'rider_id']:
      if rider_data.loc[i,'count']>=3:
        if rider_data.loc[i+1,'attendance_status'] not in (1,3,5):
          rider_data.loc[i+1,'weighted_score']=rider_data.loc[i,'weighted_score']*(1-l) + rider_data.loc[i+1,'score']*(l) 
          rider_data.loc[i+1,'count']=rider_data.loc[i,'count']+1
        else:
          rider_data.loc[i+1,'weighted_score']=rider_data.loc[i,'weighted_score']
          rider_data.loc[i+1,'count']=rider_data.loc[i,'count']
      else:
        if rider_data.loc[i+1,'attendance_status'] not in (1,3,5):
          rider_data.loc[i+1,'weighted_score']=(rider_data.loc[i+1,'score']+rider_data.loc[i,'weighted_score']*rider_data.loc[i,'count'])/(1+rider_data.loc[i,'count'])
          rider_data.loc[i+1,'count']=rider_data.loc[i,'count']+1
        else:
          rider_data.loc[i+1,'weighted_score']=rider_data.loc[i,'weighted_score']
          rider_data.loc[i+1,'count']=rider_data.loc[i,'count']
  max_score = 180
  min_score = -35
  rider_data['rating']=rider_data.apply(lambda x: 5*((min(x['weighted_score'],180)- min_score )/ (max_score-min_score)),axis=1)
  # rider_data['rating']=rider_data.apply(lambda x: min(x['rating'],4.8),axis=1)
  # rider_data.rename(columns={'date':'date'},inplace=True)
  # ind = (rider_data.rating >= 4.5)
  # rider_data.loc[ind, 'rating'] = 4.5
  rider_data=rider_data.round(1)
  rating_data=rider_data.groupby('rider_id').last().reset_index()
  rating_data=rating_data[['rider_id','cluster_id','city','date','attendance_status','total_hours','order_per_hour','n_order_count','ecom_orders','BB_orders','Pharmeasy_orders','other_orders','delivery_sla','percent_delivery_sla','tickets_count','score','count','weighted_score','rating']]
  final_data=rider_data[['rider_id','cluster_id','city','date','attendance_status','total_hours','order_per_hour','n_order_count','ecom_orders','BB_orders','Pharmeasy_orders','other_orders','delivery_sla','percent_delivery_sla','tickets_count','score','count','weighted_score','rating']]
  
  fname=root_path+'/data/rating_data.csv'
  rating_data.to_csv(fname,index=False,encoding='utf-8')
  # create_table(cnx)
  upload_data(fname,cnx)
  rating_data=rating_data.sort_values(by=['weighted_score'],ascending=['False'])
  rating_data.to_excel(writer,'ratings',index=False)
  rider_data.to_excel(writer,'raw_data',index=False)
  writer.save()



def get_order_data(cnx,BB_coeff,default_sla):
  query2 = '''SELECT 
        sr.id rider_id,
        DATE(o.scheduled_time) 'date',
        concat(rp.first_name,' ',rp.last_name) rider_name,
        o.cluster_id,
        cl.cluster_name,
        rp.city,
        rp.role,
        count(o.id) 'order_completed',
        sum(CASE WHEN (o.source=9 or ss.outlet_name like '%reverse%') THEN 1 ELSE 0 END) 'ecom_orders',
        sum(CASE WHEN ss.chain_id=85 THEN 1 ELSE 0 END) 'BB_orders',
        sum(CASE WHEN ss.chain_id=1205 THEN 1 ELSE 0 END) 'Pharmeasy_orders',
        count(o.id)-sum(CASE WHEN (o.source=9 OR ss.outlet_name like '%reverse%' or ss.chain_id=85 OR ss.chain_id=1205) THEN 1 ELSE 0 END) 'other_orders',
        ((count(o.id)-sum(CASE WHEN (o.source=9 OR ss.chain_id=85 OR ss.chain_id=1205 or ss.outlet_name like '%reverse%') THEN 1 ELSE 0 END))+0.5*sum(CASE WHEN (o.source=9 or ss.outlet_name like '%reverse%' )THEN 1 ELSE 0 END)+'''+str(BB_coeff)+'''*sum(CASE WHEN ss.chain_id=85 THEN 1 ELSE 0 END)+sum(CASE WHEN ss.chain_id=1205 THEN 1 ELSE 0 END))  'n_order_count',
        sum(CASE WHEN o.source!=9 AND ss.outlet_name not like '%reverse%' AND ss.chain_id=85 AND TIMESTAMPDIFF(MINUTE,o.scheduled_time,o.delivered_time) <= ms.delivered_sla  THEN '''+str(BB_coeff)+'''
               WHEN o.source!=9 AND ss.outlet_name not like '%reverse%' AND TIMESTAMPDIFF(MINUTE,o.scheduled_time,o.delivered_time) <= IF(ms.delivered_sla IS NULL,+'''+str(default_sla)+''',ms.delivered_sla) THEN 1
               WHEN (o.source=9 or ss.outlet_name like '%reverse%') THEN 0.5 
               ELSE 0 END) delivery_sla
          FROM coreengine_sfxrider sr
        LEFT JOIN coreengine_order o ON o.rider_id=sr.id 
        LEFT JOIN coreengine_riderprofile rp ON sr.rider_id=rp.id
        LEFT JOIN coreengine_sfxseller ss ON o.seller_id =ss.id 
        LEFT JOIN merchant_sla ms ON ms.chain_id=ss.chain_id
        left join coreengine_cluster cl on o.cluster_id=cl.id
        WHERE DATE(o.scheduled_time) = curdate()- INTERVAL 1 DAY 
                AND sr.id!=1 AND sr.status=1
        AND (o.status <6 OR o.status=8)
        AND o.cluster_id NOT IN (1,19)
        AND cl.cluster_name not like "%test%"
        AND cl.cluster_name not like "%hub%"
        and cl.cluster_name not like "%helper%" 
        and cl.cluster_name not like "%snapdeal%" 
        AND ss.outlet_name NOT LIKE "%test%"
        GROUP BY 1,2''' 
  order_data=sql.read_sql(query2, cnx)
  print "order_data",order_data.shape
  return order_data


def get_attendance_data(cnx):
  print "fetching rider_data"
  query1='''SELECT 
        ra.rider_id,
        ra.attendancedate 'date',
        concat(rp.first_name,' ',rp.last_name) rider_name,
        sr.cluster_id,
        cl.cluster_name,
        rp.city,
        rp.role,
        ra.attendancedate,
                ra.status 'attendance_status',
        sum(case when tc.category not like "%Addition%" then 1 else 0 end) 'tickets_count',
        (sum(case when tc.category not like "%Addition%" then 1 else 0 end)*-10) 'tickets',
        (CASE WHEN ra.total_working_hours<4 THEN (CASE rp.shift_type WHEN 0 THEN 4
                                                               WHEN 2 THEN 11
                                                               ELSE 9 END) 
              WHEN ra.total_working_hours>13 THEN (CASE rp.shift_type WHEN 0 THEN 4
                                                               WHEN 2 THEN 11
                                                               ELSE 9 END)
              ELSE ra.total_working_hours END) total_hours
            FROM coreengine_riderattendance ra 
            LEFT JOIN coreengine_sfxrider sr ON ra.rider_id=sr.id
            LEFT JOIN coreengine_riderprofile rp ON sr.rider_id=rp.id
            LEFT JOIN coreengine_tickets t ON sr.id=t.rider_id AND ra.attendancedate=DATE(t.date_raised)
            LEFT JOIN coreengine_cluster cl ON sr.cluster_id=cl.id
            left join coreengine_ticketcategory tc on t.category_id=tc.id
            WHERE ra.attendancedate = curdate()- INTERVAL 1 DAY 
            AND sr.id!=1 
            AND sr.status=1
            AND sr.cluster_id NOT IN (1,19)
        AND cl.cluster_name NOT LIKE "%test%"
        AND cl.cluster_name NOT LIKE "%hub%"
        and cl.cluster_name not like "%helper%" 
        and cl.cluster_name not like "%snapdeal%" 
        
            GROUP BY 1,2;
        '''
  attendance_data=sql.read_sql(query1, cnx)
  print "attendance_data",attendance_data.shape
  return attendance_data

def get_BB_data(cnx):
  print "Fetching BB_data"
  query3='''SELECT sum(t1.total_orders) total_orders,
             sum(t2.attendance) total_attendance,
             sum(t1.total_orders)/sum(t2.attendance) eff 
        FROM
        (
          SELECT o.rider_id, DATE(o.scheduled_time) DATE, count(o.id) total_orders,sum(CASE WHEN ss.chain_id=85 THEN 1 ELSE 0 END) BB_orders FROM coreengine_order o
          INNER JOIN coreengine_sfxseller ss ON o.seller_id=ss.id
          AND DATE(o.scheduled_time) BETWEEN curdate() - INTERVAL 30 DAY AND curdate() - INTERVAL 1 DAY
          GROUP BY 1,2
          HAVING BB_orders=total_orders
        )t1,
        (
          SELECT ra.rider_id, attendancedate,
          (CASE WHEN ra.status=0 AND rp.role='FT' THEN 1 
               WHEN ra.status=0 AND rp.role='PRT' THEN 0.5 ELSE 0 END) attendance
          FROM coreengine_riderattendance ra INNER JOIN coreengine_sfxrider sr ON ra.rider_id=sr.id
          INNER JOIN coreengine_riderprofile rp ON sr.rider_id=rp.id
          WHERE
          ra.attendancedate BETWEEN curdate() - INTERVAL 30 DAY AND curdate() - INTERVAL 1 DAY 
        ) t2 
        WHERE t1.rider_id=t2.rider_id AND t1.date=t2.attendancedate;'''
  BB_data=sql.read_sql(query3,cnx)
  print BB_data.shape
  return BB_data




def get_HL_data(cnx):
  print "fetching HL_data"
  query4='''SELECT sum(t1.total_orders) total_order,
           sum(t2.attendance) total_attendance,
           sum(t1.total_orders)/sum(t2.attendance) eff 
        FROM
        ( 
          SELECT o.rider_id, DATE(o.scheduled_time) DATE, count(o.id) total_orders,sum(CASE WHEN ss.chain_id=85 THEN 1 ELSE 0 END) BB_orders,
          sum(CASE WHEN o.source=9 or ss.outlet_name like '%reverse%' THEN 1 ELSE 0 END) ecom_orders FROM coreengine_order o
          INNER JOIN coreengine_sfxseller ss ON o.seller_id=ss.id
          AND DATE(o.scheduled_time) BETWEEN curdate() - INTERVAL 30 DAY AND curdate() - INTERVAL 1 DAY
          GROUP BY 1,2
          HAVING BB_orders=0
          AND ecom_orders=0
        ) t1,
        (
          SELECT ra.rider_id, attendancedate,
          (CASE WHEN ra.status=0 AND rp.role='FT' THEN 1 
               WHEN ra.status=0 AND rp.role='PRT' THEN 0.5 ELSE 0 END) attendance
          FROM coreengine_riderattendance ra INNER JOIN coreengine_sfxrider sr ON ra.rider_id=sr.id
          INNER JOIN coreengine_riderprofile rp ON sr.rider_id=rp.id
          WHERE
          ra.attendancedate BETWEEN curdate() - INTERVAL 30 DAY AND curdate() - INTERVAL 1 DAY
            )t2
        WHERE t1.rider_id=t2.rider_id  AND t1.date=t2.attendancedate;     
        '''
  HL_data=sql.read_sql(query4,cnx)
  print HL_data.shape
  return HL_data

def get_att_bonus(row):
  if (row['attendance_status']==0 and row['n_order_count']>0):
    return 10
  elif (row['attendance_status']==2 or row['attendance_status']==4):
    return -35
  else:
    return 0

def upload_data(fname, cnx):
    cursor = cnx.cursor()
    print "uploading data"
    query = '''load data local infile "''' + fname + '''" replace 
        into table rider_rating_m2 fields terminated by ',' enclosed by '\"' lines terminated by 
        '\n' ignore 1 lines (rider_id,
         cluster_id,city,date,attendance_status,total_hours,order_per_hour,n_order_count,ecom_orders,BB_orders,
         Pharmeasy_orders,other_orders,delivery_sla,percent_delivery_sla,tickets_count,score,count,weighted_score,rating
         );'''
    # print query
    cursor.execute(query)
    cnx.commit()
    cursor.close()

def drop_table(cnx):
  print "inside drop_table"
  cursor = cnx.cursor()
  query_droptable='''DROP TABLE `rider_rating_m2` ;'''
  cursor.execute(query_droptable)
  cnx.commit()
  cursor.close()

def create_table(cnx):
  print "inside create_table"
  cursor = cnx.cursor()
  query_createtable='''create table `rider_rating_m2`
    (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `rider_id` int(11) NOT NULL references coreengine_sfxrider(id),
    `cluster_id` int(11) NOT NULL references coreengine_cluster(id),
    `city` varchar(5) NOT NULL,
    `date` date NOT NULL,
    `attendance_status` int(11) DEFAULT NULL,
    `total_hours` int(11) DEFAULT NULL,
    `order_per_hour` float(5,2) DEFAULT NULL,
    `n_order_count` int(11) DEFAULT NULL,
    `ecom_orders` int(11) DEFAULT NULL,
    `BB_orders` int(11) DEFAULT NULL,
    `Pharmeasy_orders` int(11) DEFAULT NULL,
    `other_orders` int(11) DEFAULT NULL,
    `delivery_sla` float(5,2) DEFAULT NULL,
    `percent_delivery_sla` float(5,2) DEFAULT NULL,
    `tickets_count` int(11) DEFAULT NULL,
    `score` float(5,2) DEFAULT NULL,
    `count` int(11) DEFAULT NULL,
    `weighted_score` float(5,2) DEFAULT NULL,
    `rating` float(5,2) DEFAULT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `un` (`rider_id`,`date`)
    )ENGINE=InnoDB DEFAULT CHARSET=latin1;
  '''
  cursor.execute(query_createtable)
  cnx.commit()
  cursor.close()

if __name__ == "__main__":
    main()

