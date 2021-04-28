import configparser

config = configparser.ConfigParser()
config.read('dwh.cfg')

##############################################################

contest_table_create = """
    CREATE TABLE IF NOT EXISTS contest (
        contestid int PRIMARY KEY,
        draftgroupid int,
        name varchar,
        entryFee numeric,
        entries int,
        maximumEntries int,
        maximumEntriesPerUser int,
        totalPayouts numeric,
        contestStateDetail varchar,
        conteststarttime varchar
    )
"""

contest_table_copy = ("""
    COPY contest
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON {}
    REGION 'us-east-1'
""").format(config.get('S3', 'CONTEST_DATA'), 
            config.get('IAM_ROLE', 'ARN'), 
            config.get('S3', 'CONTEST_JSONPATH'))

#######################################################

payout_table_create = """
    CREATE TABLE IF NOT EXISTS payout (
        contestid int PRIMARY KEY,
        rank int,
        prize numeric
    )
"""

payout_table_copy = ("""
    COPY payout
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON 'auto ignorecase'
    REGION 'us-east-1'
""").format(config.get('S3', 'PAYOUT_DATA'), 
            config.get('IAM_ROLE', 'ARN'))
			
########################################################
			
player_lineups_create = ("""
	CREATE TABLE IF NOT EXISTS player_lineup
	(
		contestid int8, 
		entryid int8,
		player varchar, 
		cpt varchar, 
		topl varchar, 
		jng varchar,
		mid varchar,
		adc varchar,
		sup varchar,
		team varchar
	)
""")

player_lineup_insert = ("""
	INSERT INTO player_lineup (contestid, entryid, player, cpt, topl, jng, mid, adc, sup, team)
	SELECT 	DISTINCT contestid, entryid, player, cpt, topl, jng, mid, adc, sup, team
	FROM contestentry ce
"""
)

#########################################################

player_payout_create = ("""
	CREATE TABLE IF NOT EXISTS player_payout 
	(
	  contestid int8, 
	  entryid int8, 
	  rank int8, 
	  player varchar, 
	  entryfee int8, 
	  prize int8 
	)
""")

player_payout_insert = ("""
	INSERT INTO player_payout (contestid, entryid, rank, player, entryfee, prize)
	SELECT 
		DISTINCT ce.contestid, ce.entryid, ce.rank, ce.player, c.entryfee, p.prize
	FROM contest c
	JOIN contestentry ce on c.contestid = ce.contestid
	LEFT JOIN
	(
	  SELECT ce.contestid, ce.rank, ROUND(AVG(NVL(p.prize, 0))) as prize
	  FROM
	  (
		  SELECT row_number() over(partition by contestid order by points desc) as row_num, *
		  FROM public.contestentry 
	  ) ce
	  LEFT JOIN payout p
	  ON 	ce.contestid = p.contestid and ce.row_num = p.rank
	  GROUP BY ce.contestid, ce.rank
	) p
	on ce.contestid = p.contestid and ce.rank = p.rank
""")


			
			
