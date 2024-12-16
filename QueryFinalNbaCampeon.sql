USE nba_campeon;
USE nba_campeon;

-- Crear la tabla team
CREATE TABLE team (
    team_id INT PRIMARY KEY,
    full_name NVARCHAR(100),
    abbreviation NVARCHAR(10),
    nickname NVARCHAR(50),
    city NVARCHAR(50),
    state NVARCHAR(50),
    year_founded INT
);

-- Crear la tabla game
CREATE TABLE game (
    game_id INT PRIMARY KEY,                      -- Identificador único del juego
    season_id NVARCHAR(20),
    team_id_home INT,
    team_abbreviation_home NVARCHAR(10),
    team_name_home NVARCHAR(100),
    game_date DATE,
    matchup_home NVARCHAR(50),
    wl_home NVARCHAR(10),
    min NVARCHAR(10),
    fgm_home INT,
    fga_home INT,
    fg_pct_home FLOAT,
    fg3m_home INT,
    fg3a_home INT,
    fg3_pct_home FLOAT,
    ftm_home INT,
    fta_home INT,
    ft_pct_home FLOAT,
    oreb_home INT,
    dreb_home INT,
    reb_home INT,
    ast_home INT,
    stl_home INT,
    blk_home INT,
    tov_home INT,
    pf_home INT,
    pts_home INT,
    plus_minus_home FLOAT,
    video_available_home BIT,
    team_id_away INT,
    team_abbreviation_away NVARCHAR(10),
    team_name_away NVARCHAR(100),
    matchup_away NVARCHAR(50),
    wl_away NVARCHAR(10),
    fgm_away INT,
    fga_away INT,
    fg_pct_away FLOAT,
    fg3m_away INT,
    fg3a_away INT,
    fg3_pct_away FLOAT,
    ftm_away INT,
    fta_away INT,
    ft_pct_away FLOAT,
    oreb_away INT,
    dreb_away INT,
    reb_away INT,
    ast_away INT,
    stl_away INT,
    blk_away INT,
    tov_away INT,
    pf_away INT,
    pts_away INT,
    plus_minus_away FLOAT,
    video_available_away BIT,
    season_type NVARCHAR(20),
    FOREIGN KEY (team_id_home) REFERENCES team(team_id),
    FOREIGN KEY (team_id_away) REFERENCES team(team_id)
);

-- Crear la tabla game_summary
CREATE TABLE game_summary (
    game_summary_id INT PRIMARY KEY IDENTITY,
    game_date_est DATE,
    game_sequence INT,
    game_id INT,
    game_status_id INT,
    game_status_text NVARCHAR(50),
    gamecode NVARCHAR(50),
    home_team_id INT,
    visitor_team_id INT,
    season NVARCHAR(10),
    live_period INT,
    live_pc_time NVARCHAR(10),
    natl_tv_broadcaster_abbreviation NVARCHAR(50),
    live_period_time_bcast NVARCHAR(10),
    wh_status INT,
    FOREIGN KEY (game_id) REFERENCES game(game_id),
    FOREIGN KEY (home_team_id) REFERENCES team(team_id),
    FOREIGN KEY (visitor_team_id) REFERENCES team(team_id)
);

-- Crear la tabla line_score
CREATE TABLE line_score (
    line_score_id INT PRIMARY KEY IDENTITY,
    game_id INT,
    team_id_home INT,
    team_abbreviation_home NVARCHAR(10),
    team_city_name_home NVARCHAR(50),
    team_nickname_home NVARCHAR(50),
    team_wins_losses_home NVARCHAR(20),
    pts_qtr1_home INT,
    pts_qtr2_home INT,
    pts_qtr3_home INT,
    pts_qtr4_home INT,
    pts_ot1_home INT,
    pts_ot2_home INT,
    pts_ot3_home INT,
    pts_ot4_home INT,
    pts_ot5_home INT,
    pts_ot6_home INT,
    pts_ot7_home INT,
    pts_ot8_home INT,
    pts_ot9_home INT,
    pts_ot10_home INT,
    pts_home INT,
    team_id_away INT,
    team_abbreviation_away NVARCHAR(10),
    team_city_name_away NVARCHAR(50),
    team_nickname_away NVARCHAR(50),
    team_wins_losses_away NVARCHAR(20),
    pts_qtr1_away INT,
    pts_qtr2_away INT,
    pts_qtr3_away INT,
    pts_qtr4_away INT,
    pts_ot1_away INT,
    pts_ot2_away INT,
    pts_ot3_away INT,
    pts_ot4_away INT,
    pts_ot5_away INT,
    pts_ot6_away INT,
    pts_ot7_away INT,
    pts_ot8_away INT,
    pts_ot9_away INT,
    pts_ot10_away INT,
    pts_away INT,
    FOREIGN KEY (game_id) REFERENCES game(game_id),
    FOREIGN KEY (team_id_home) REFERENCES team(team_id),
    FOREIGN KEY (team_id_away) REFERENCES team(team_id)
);

-- Crear la tabla other_stats
CREATE TABLE other_stats (
    stats_id INT PRIMARY KEY IDENTITY,
    game_id INT,
    league_id NVARCHAR(10),
    team_id_home INT,
    team_abbreviation_home NVARCHAR(10),
    team_city_home NVARCHAR(50),
    pts_paint_home INT,
    pts_2nd_chance_home INT,
    pts_fb_home INT,
    largest_lead_home INT,
    lead_changes INT,
    times_tied INT,
    team_turnovers_home INT,
    total_turnovers_home INT,
    team_rebounds_home INT,
    pts_off_to_home INT,
    team_id_away INT,
    team_abbreviation_away NVARCHAR(10),
    team_city_away NVARCHAR(50),
    pts_paint_away INT,
    pts_2nd_chance_away INT,
    pts_fb_away INT,
    largest_lead_away INT,
    team_turnovers_away INT,
    total_turnovers_away INT,
    team_rebounds_away INT,
    pts_off_to_away INT,
    FOREIGN KEY (game_id) REFERENCES game(game_id),
    FOREIGN KEY (team_id_home) REFERENCES team(team_id),
    FOREIGN KEY (team_id_away) REFERENCES team(team_id)
);

-- Crear la tabla team_details
CREATE TABLE team_details (
    team_details_id INT PRIMARY KEY IDENTITY,
    team_id INT,
    abbreviation NVARCHAR(10),
    nickname NVARCHAR(50),
    yearfounded INT,
    city NVARCHAR(50),
    arena NVARCHAR(100),
    arenacapacity INT,
    owner NVARCHAR(100),
    generalmanager NVARCHAR(100),
    headcoach NVARCHAR(100),
    dleagueaffiliation NVARCHAR(100),
    facebook NVARCHAR(100),
    instagram NVARCHAR(100),
    twitter NVARCHAR(100),
    FOREIGN KEY (team_id) REFERENCES team(team_id)
);

-- Crear la tabla team_history
CREATE TABLE team_history (
    history_id INT PRIMARY KEY IDENTITY,
    team_id INT,
    city NVARCHAR(100),
    nickname NVARCHAR(100),
    year_founded INT,
    year_active_till INT,
    FOREIGN KEY (team_id) REFERENCES team(team_id)
);

-- Crear la tabla game_info
CREATE TABLE game_info (
    info_id INT PRIMARY KEY IDENTITY,
    game_id INT,
    game_date DATE,
    attendance INT,
    game_time TIME,
    FOREIGN KEY (game_id) REFERENCES game(game_id)
);

select * from game;
select * from team;
select * from game_info;
select * from game_summary;
select * from line_score;
select * from other_stats;
select * from team_details;
select * from team_history;



ALTER TABLE game_summary
ALTER COLUMN live_pc_time VARCHAR(50);

SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'line_score';


ALTER TABLE line_score
ADD game_date_est DATE, 
    game_sequence INT;

SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'other_stats';

ALTER TABLE other_stats
ALTER COLUMN live_pc_time datetime2;

ALTER TABLE other_stats
ADD live_pc_time datetime2;


------------------------------------------------------------------------

--Creacion de Foraneas-

-- Tabla game
ALTER TABLE game
ADD CONSTRAINT FK_game_team_home FOREIGN KEY (team_id_home) REFERENCES team (team_id),
    CONSTRAINT FK_game_team_away FOREIGN KEY (team_id_away) REFERENCES team (team_id);

-- Tabla game_summary
ALTER TABLE game_summary
ADD CONSTRAINT FK_game_summary_game FOREIGN KEY (game_id) REFERENCES game (game_id),
    CONSTRAINT FK_game_summary_home_team FOREIGN KEY (home_team_id) REFERENCES team (team_id),
    CONSTRAINT FK_game_summary_visitor_team FOREIGN KEY (visitor_team_id) REFERENCES team (team_id);

-- Tabla line_score
ALTER TABLE line_score
ADD CONSTRAINT FK_line_score_game FOREIGN KEY (game_id) REFERENCES game (game_id),
    CONSTRAINT FK_line_score_team_home FOREIGN KEY (team_id_home) REFERENCES team (team_id),
    CONSTRAINT FK_line_score_team_away FOREIGN KEY (team_id_away) REFERENCES team (team_id);

-- Tabla other_stats
ALTER TABLE other_stats
ADD CONSTRAINT FK_other_stats_game FOREIGN KEY (game_id) REFERENCES game (game_id),
    CONSTRAINT FK_other_stats_team_home FOREIGN KEY (team_id_home) REFERENCES team (team_id),
    CONSTRAINT FK_other_stats_team_away FOREIGN KEY (team_id_away) REFERENCES team (team_id);

-- Tabla team_history
ALTER TABLE team_history
ADD CONSTRAINT FK_team_history_team FOREIGN KEY (team_id) REFERENCES team (team_id);

-- Tabla game_info
ALTER TABLE game_info
ADD CONSTRAINT FK_game_info_game FOREIGN KEY (game_id) REFERENCES game (game_id);

----------------------------------------------------------------------------------

-- Identificar registros problemáticos
SELECT DISTINCT game_id
FROM line_score
WHERE game_id NOT IN (SELECT game_id FROM game);

-- Resolver: eliminar los registros problemáticos en `line_score`
DELETE FROM line_score
WHERE game_id NOT IN (SELECT game_id FROM game);








-- Identificar registros problemáticos para `team_id_home`
SELECT DISTINCT team_id_home
FROM line_score
WHERE team_id_home NOT IN (SELECT team_id FROM team);

-- Resolver: eliminar los registros problemáticos en `line_score` (team_id_home)
DELETE FROM line_score
WHERE team_id_home NOT IN (SELECT team_id FROM team);

-- Identificar registros problemáticos para `team_id_away`
SELECT DISTINCT team_id_away
FROM line_score
WHERE team_id_away NOT IN (SELECT team_id FROM team);

-- Resolver: eliminar los registros problemáticos en `line_score` (team_id_away)
DELETE FROM line_score
WHERE team_id_away NOT IN (SELECT team_id FROM team);




-- Identificar registros problemáticos
SELECT DISTINCT game_id
FROM game_summary
WHERE game_id NOT IN (SELECT game_id FROM game);

-- Resolver: eliminar los registros problemáticos en `game_summary`
DELETE FROM game_summary
WHERE game_id NOT IN (SELECT game_id FROM game);



-- Identificar registros problemáticos para `home_team_id`
SELECT DISTINCT home_team_id
FROM game_summary
WHERE home_team_id NOT IN (SELECT team_id FROM team);

-- Resolver: eliminar los registros problemáticos en `game_summary` (home_team_id)
DELETE FROM game_summary
WHERE home_team_id NOT IN (SELECT team_id FROM team);

-- Identificar registros problemáticos para `visitor_team_id`
SELECT DISTINCT visitor_team_id
FROM game_summary
WHERE visitor_team_id NOT IN (SELECT team_id FROM team);

-- Resolver: eliminar los registros problemáticos en `game_summary` (visitor_team_id)
DELETE FROM game_summary
WHERE visitor_team_id NOT IN (SELECT team_id FROM team);



-- Identificar registros problemáticos
SELECT DISTINCT team_id
FROM team_history
WHERE team_id NOT IN (SELECT team_id FROM team);

-- Resolver: eliminar los registros problemáticos en `team_history`
DELETE FROM team_history
WHERE team_id NOT IN (SELECT team_id FROM team);



-- Identificar registros problemáticos
SELECT DISTINCT game_id
FROM other_stats
WHERE game_id NOT IN (SELECT game_id FROM game);

-- Resolver: eliminar los registros problemáticos en `other_stats`
DELETE FROM other_stats
WHERE game_id NOT IN (SELECT game_id FROM game);

-- Identificar registros problemáticos para `team_id_home`
SELECT DISTINCT team_id_home
FROM other_stats
WHERE team_id_home NOT IN (SELECT team_id FROM team);

-- Resolver: eliminar los registros problemáticos en `other_stats` (team_id_home)
DELETE FROM other_stats
WHERE team_id_home NOT IN (SELECT team_id FROM team);





-- Identificar registros problemáticos para `team_id_away`
SELECT DISTINCT team_id_away
FROM other_stats
WHERE team_id_away NOT IN (SELECT team_id FROM team);

-- Resolver: eliminar los registros problemáticos en `other_stats` (team_id_away)
DELETE FROM other_stats
WHERE team_id_away NOT IN (SELECT team_id FROM team);





-- Identificar registros problemáticos
SELECT DISTINCT game_id
FROM game_info
WHERE game_id NOT IN (SELECT game_id FROM game);

-- Resolver: eliminar los registros problemáticos en `game_info`
DELETE FROM game_info
WHERE game_id NOT IN (SELECT game_id FROM game);
