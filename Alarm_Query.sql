-------------------------------------------------------------------
--ALARM - 1 

select assetId,
 (1 + CAST(assetPrice AS DEC(38,8))) - 1 as assetPrice,
volume24h,change1h,change24h,change7d,astatus,updated,sira
from
( 
select assetId,assetPrice,volume24h,change1h,change24h,change7d,astatus,DATEADD(hour,3,updated) as updated
,ROW_NUMBER()OVER(PARTITION BY assetId order by DATEADD(hour,3,updated) desc) as sira  from test.dbo.assets
)a
where sira=1
and
(
a.change1h<-1
or
a.change24h<-1
)


-------------------------------------------------------------------

--ALARM - 2

select
cBase.excId,
cBase.symbol,
cBase.base,
cBase.quote,
cBase.price,
cBase.change24h,
cBase.spread,
cBase.vol24h,
cBase.status,
cBase.updated,
case 
	when cBase.base='ADA' then 2.18833333
	when cBase.base='ALGO' then 1.912345
	when cBase.base='BTC' then 57680.123999
	when cBase.base='ETH' then 3795.328458
	when cBase.base='FX' then 1.302857
	when cBase.base='LINK' then 27.245025
	when cBase.base='LTC' then 182.614852
	when cBase.base='SHIB' then 0.00002932
	when cBase.base='USDT' then 1.120855
	when cBase.base='WBTC' then 57578.95
	when cBase.base='WBTC' then 57578.95
else 0
end tPrice
from
(
select
excId,
symbol,
base,
quote,
priceUnc,
--price,
 (1 + CAST(price AS DEC(38,8))) - 1 as price,
change24h,
spread,
vol24h,
status,
created,
DATEADD(hour,3,updated) as updated,
ROW_NUMBER()OVER(PARTITION BY excId,base order by DATEADD(hour,3,updated) desc) as sira 
from test.dbo.markets
WHERE
excId='COINBASE'
)cBase
where
cBase.sira=1



-------------------------------------------------------------------

--ALARM - 3

SELECT top 1 x.assetId,x.assetPrice,x.prePrice,x.lostPers,x.uDate,x.uHour,x.fupdated 
FROM
(
select rec.assetId,rec.assetPrice,rec.updated,rec.uDate,rec.uHour,rec.fupdated 
,rec.prePrice
,rec.assetPrice-rec.prePrice as fark
,ROUND((rec.assetPrice-rec.prePrice)*100/rec.prePrice ,3) lostPers
from
(
select 
assetId,
(1 + CAST(assetPrice AS DEC(38,8))) - 1 as assetPrice,
volume24h,
change1h,
change24h,
change7d,
astatus,
DATEADD(hour,3,updated) as updated,
FORMAT(DATEADD(hour,3,updated),'yyyy-MM-dd HH:mm') AS fupdated,
CONVERT(DATE, DATEADD(hour,3,updated))uDate,
FORMAT(DATEADD(hour,3,updated),'HH:mm') uHour,
LAG((1 + CAST(assetPrice AS DEC(38,8))) - 1 )over(partition by assetId order by FORMAT(DATEADD(hour,3,updated),'yyyy-MM-dd HH:mm') asc) as prePrice
from test.dbo.assets
where DATEADD(hour,3,updated) >= DATEADD(HH, -24, GETDATE())
)rec
--where
--rec.assetId='BTC'
) X
where
x.lostPers is not NULL
and
x.fupdated> '2021-10-14 16:20'
ORDER BY x.lostPers asc


-------------------------------------------------------------------

--ALARM - 4


SELECT chngHour.assetId,SUM(chngHour.priPers)/COUNT(chngHour.priPers) as avgHour FROM
(
SELECT x.assetId,x.assetPrice,x.prePrice,x.priPers,x.fupdated,x.updated
FROM
(
select 
assetId,
(1 + CAST(assetPrice AS DEC(38,8))) - 1 as assetPrice,
volume24h,
change1h,
change24h,
change7d,
astatus,
DATEADD(hour,3,updated) as updated,
FORMAT(DATEADD(hour,3,updated),'yyyy-MM-dd HH:mm') AS fupdated,
CONVERT(DATE, DATEADD(hour,3,updated))uDate,
FORMAT(DATEADD(hour,3,updated),'HH:mm') uHour,
LAG((1 + CAST(assetPrice AS DEC(38,8))) - 1 )over(partition by assetId order by FORMAT(DATEADD(hour,3,updated),'yyyy-MM-dd HH:mm') asc) as prePrice
,
ROUND(
(((1 + CAST(assetPrice AS DEC(38,8))) - 1)-LAG((1 + CAST(assetPrice AS DEC(38,8))) - 1 )over(partition by assetId order by FORMAT(DATEADD(hour,3,updated),'yyyy-MM-dd HH:mm') asc))*100
/LAG((1 + CAST(assetPrice AS DEC(38,8))) - 1 )over(partition by assetId order by FORMAT(DATEADD(hour,3,updated),'yyyy-MM-dd HH:mm') asc),3) priPers
from test.dbo.assets
where DATEADD(hour,3,updated) >= DATEADD(HH, -24, GETDATE())
--and
--assetId='BTC'
) X
RIGHT OUTER JOIN
(
SELECT top 1 
FORMAT(DATEADD(HH, -1, x.fupdated),'yyyy-MM-dd HH:mm') as firstTime
,x.fupdated as lastTime
FROM
(
select rec.assetId,rec.assetPrice,rec.updated,rec.uDate,rec.uHour,rec.fupdated 
,rec.prePrice
,rec.assetPrice-rec.prePrice as fark
,ROUND((rec.assetPrice-rec.prePrice)*100/rec.prePrice ,3) lostPers
from
(
select 
assetId,
(1 + CAST(assetPrice AS DEC(38,8))) - 1 as assetPrice,
volume24h,
change1h,
change24h,
change7d,
astatus,
DATEADD(hour,3,updated) as updated,
FORMAT(DATEADD(hour,3,updated),'yyyy-MM-dd HH:mm') AS fupdated,
CONVERT(DATE, DATEADD(hour,3,updated))uDate,
FORMAT(DATEADD(hour,3,updated),'HH:mm') uHour,
LAG((1 + CAST(assetPrice AS DEC(38,8))) - 1 )over(partition by assetId order by FORMAT(DATEADD(hour,3,updated),'yyyy-MM-dd HH:mm') asc) as prePrice
from test.dbo.assets
where DATEADD(hour,3,updated) >= DATEADD(HH, -24, GETDATE())
)rec
) X
where
x.lostPers is not NULL
and
x.fupdated> '2021-10-14 16:20'
ORDER BY x.lostPers asc
) C ON x.fupdated between C.firstTime AND C.lastTime
where 
x.prePrice is not null
)chngHour
group by chngHour.assetId


-------------------------------------------------------------------


--ALARM - 5


SELECT ASSET_DIFF.assetId,ASSET_DIFF.pMin,ASSET_DIFF.pMax,ASSET_DIFF.pDiff 
,ASSET_LIST.assetPrice
,ASSET_LIST.prePrice
,ASSET_LIST.fupdated
FROM 
(
SELECT TOP 1 SCA.assetId,min(sca.priPers) pMin,max(sca.priPers) pMax,
max(sca.priPers)-min(sca.priPers)  pDiff
FROM
(
select 
assetId,
(1 + CAST(assetPrice AS DEC(38,8))) - 1 as assetPrice,
volume24h,
change1h,
change24h,
change7d,
astatus,
DATEADD(hour,3,updated) as updated,
FORMAT(DATEADD(hour,3,updated),'yyyy-MM-dd HH:mm') AS fupdated,
CONVERT(DATE, DATEADD(hour,3,updated))uDate,
FORMAT(DATEADD(hour,3,updated),'HH:mm') uHour,
LAG((1 + CAST(assetPrice AS DEC(38,8))) - 1 )over(partition by assetId order by FORMAT(DATEADD(hour,3,updated),'yyyy-MM-dd HH:mm') asc) as prePrice
,
ROUND(
(((1 + CAST(assetPrice AS DEC(38,8))) - 1)-LAG((1 + CAST(assetPrice AS DEC(38,8))) - 1 )over(partition by assetId order by FORMAT(DATEADD(hour,3,updated),'yyyy-MM-dd HH:mm') asc)),3) priDiff
,
ROUND(
(((1 + CAST(assetPrice AS DEC(38,8))) - 1)-LAG((1 + CAST(assetPrice AS DEC(38,8))) - 1 )over(partition by assetId order by FORMAT(DATEADD(hour,3,updated),'yyyy-MM-dd HH:mm') asc))*100
/LAG((1 + CAST(assetPrice AS DEC(38,8))) - 1 )over(partition by assetId order by FORMAT(DATEADD(hour,3,updated),'yyyy-MM-dd HH:mm') asc),3) priPers
from test.dbo.assets
where DATEADD(hour,3,updated) >= DATEADD(HH, -24, GETDATE())
--and
--assetId='BTC'
) SCA
WHERE
SCA.prePrice is not null
AND
SCA.fupdated>'2021-10-14 16:20'
group by SCA.assetId
order by max(sca.priPers)-min(sca.priPers) desc
) ASSET_DIFF
JOIN
(
select 
assetId,
(1 + CAST(assetPrice AS DEC(38,8))) - 1 as assetPrice,
volume24h,
change1h,
change24h,
change7d,
astatus,
DATEADD(hour,3,updated) as updated,
FORMAT(DATEADD(hour,3,updated),'yyyy-MM-dd HH:mm') AS fupdated,
CONVERT(DATE, DATEADD(hour,3,updated))uDate,
FORMAT(DATEADD(hour,3,updated),'HH:mm') uHour,
LAG((1 + CAST(assetPrice AS DEC(38,8))) - 1 )over(partition by assetId order by FORMAT(DATEADD(hour,3,updated),'yyyy-MM-dd HH:mm') asc) as prePrice
,
ROUND(
(((1 + CAST(assetPrice AS DEC(38,8))) - 1)-LAG((1 + CAST(assetPrice AS DEC(38,8))) - 1 )over(partition by assetId order by FORMAT(DATEADD(hour,3,updated),'yyyy-MM-dd HH:mm') asc)),3) priDiff
,
ROUND(
(((1 + CAST(assetPrice AS DEC(38,8))) - 1)-LAG((1 + CAST(assetPrice AS DEC(38,8))) - 1 )over(partition by assetId order by FORMAT(DATEADD(hour,3,updated),'yyyy-MM-dd HH:mm') asc))*100
/LAG((1 + CAST(assetPrice AS DEC(38,8))) - 1 )over(partition by assetId order by FORMAT(DATEADD(hour,3,updated),'yyyy-MM-dd HH:mm') asc),3) priPers
from test.dbo.assets
where DATEADD(hour,3,updated) >= DATEADD(HH, -24, GETDATE())
AND FORMAT(DATEADD(hour,3,updated),'yyyy-MM-dd HH:mm') > '2021-10-14 16:20'
--AND assetId='FX'
)ASSET_LIST ON ASSET_DIFF.assetId=ASSET_LIST.assetId AND ASSET_DIFF.pMin=ASSET_LIST.priPers