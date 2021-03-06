-------------------------------------------------------------

-- ANLIK PORTFÖY DEĞER BİLGİSİ

select assetId,
 (1 + CAST(assetPrice AS DEC(38,8))) - 1 as assetPrice,
volume24h,change1h,change24h,change7d,astatus,updated,sira
from
( 
select assetId,assetPrice,volume24h,change1h,change24h,change7d,astatus,DATEADD(hour,3,updated) as updated
,ROW_NUMBER()OVER(PARTITION BY assetId order by DATEADD(hour,3,updated) desc) as sira  from test.dbo.assets
)a
where sira=1


---------------------------------------------------------------

--SON 24 SAATTE EN COK ARTIŞ GÖSTEREN VARLIK

select 
assetId, (1 + CAST(assetPrice AS DEC(38,8))) - 1 as assetPrice,volume24h,change1h,change24h,change7d,astatus,
DATEADD(hour,3,updated) as updated,
FORMAT(DATEADD(hour,3,updated),'yyyy-MM-dd HH:mm') as fupdated,
CONVERT(DATE, DATEADD(hour,3,updated))uDate,
FORMAT(DATEADD(hour,3,updated),'HH:mm') uHour
from test.dbo.assets
where DATEADD(hour,3,updated) >= DATEADD(HH, -24, GETDATE())
	AND assetId in
	(
		select mWin.assetId from
		(
			select actual.assetId,actual.change24h,row_number()over(order by actual.change24h desc) mRow
			from
			( 
				select assetId,assetPrice,volume24h,change1h,change24h,change7d,astatus,DATEADD(hour,3,updated) as updated
				,ROW_NUMBER()OVER(PARTITION BY assetId order by DATEADD(hour,3,updated) desc) as sira  
				from test.dbo.assets
			)actual
			where actual.sira=1 and actual.change24h>0
		) mWin
		where mWin.mRow=1
	)
order by updated asc


----------------------------------------------------------------------------------------------

--SON 24 SAATTE EN ÇOK KAYIP GÖSTEREN VARLIK



select 
assetId, (1 + CAST(assetPrice AS DEC(38,8))) - 1 as assetPrice,volume24h,change1h,change24h,change7d,astatus,
DATEADD(hour,3,updated) as updated,
CONVERT(DATE, DATEADD(hour,3,updated))uDate,
FORMAT(DATEADD(hour,3,updated),'HH:mm') uHour
from test.dbo.assets
where DATEADD(hour,3,updated) >= DATEADD(HH, -24, GETDATE())
	AND assetId in
	(
		select mWin.assetId from
		(
			select actual.assetId,actual.change24h,row_number()over(order by actual.change24h asc) mRow
			from
			( 
				select assetId,assetPrice,volume24h,change1h,change24h,change7d,astatus,DATEADD(hour,3,updated) as updated
				,ROW_NUMBER()OVER(PARTITION BY assetId order by DATEADD(hour,3,updated) desc) as sira  
				from test.dbo.assets
			)actual
			where actual.sira=1 and actual.change24h<0
		) mWin
		where mWin.mRow=1
	)
order by updated desc


-----------------------------------------------------------------------------------------------------

-- PORTFÖYDEKİ TÜM VARLIKLARIN USD CİNSİNDEN SON 24 SAATTEKİ VARLIK TOPLAMI

SELECT
allAssets.updated,
allAssets.uDate,
allAssets.uHour,
sum(allAssets.volume24h) as volume
FROM
(
select 
assetId, (1 + CAST(assetPrice AS DEC(38,8))) - 1 as assetPrice,volume24h,change1h,change24h,change7d,astatus,
DATEADD(hour,3,updated) as updated,
CONVERT(DATE, DATEADD(hour,3,updated))uDate,
FORMAT(DATEADD(hour,3,updated),'HH:mm') uHour
from test.dbo.assets
where DATEADD(hour,3,updated) >= DATEADD(HH, -24, GETDATE())
)allAssets
group by
allAssets.updated,allAssets.uDate,allAssets.uHour
order by 
allAssets.uDate desc,allAssets.uHour desc