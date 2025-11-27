-- Cleanup

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE Clients CASCADE CONSTRAINTS';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE Clients_Statique CASCADE CONSTRAINTS';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE Clients_Dynamique CASCADE CONSTRAINTS';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP VIEW Clients_Complet';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/


-- Création des tables
CREATE TABLE Clients (
    Client_ID NUMBER PRIMARY KEY,
    Nom VARCHAR2(100),
    Adresse VARCHAR2(255),
    Pays VARCHAR2(50),
    Date_Inscription DATE
)
PARTITION BY LIST (Pays) (
    PARTITION partition_france VALUES ('France'),
    PARTITION partition_usa VALUES ('USA'),
    PARTITION partition_canada VALUES ('Canada'),
    PARTITION partition_autres VALUES (DEFAULT)
);


-- 2. Insertion des données dans Clients
INSERT INTO Clients VALUES (1, 'Alice Dupont', '10 rue des Lilas, Paris', 'France', TO_DATE('2022-01-10', 'YYYY-MM-DD'));
INSERT INTO Clients VALUES (2, 'John Smith', '123 Main St, New York', 'USA', TO_DATE('2021-05-15', 'YYYY-MM-DD'));
INSERT INTO Clients VALUES (3, 'Marie Curie', '5 avenue des Champs, Montreal', 'Canada', TO_DATE('2023-03-20', 'YYYY-MM-DD'));
INSERT INTO Clients VALUES (4, 'Carlos Santos', '50 Rua de Lisboa', 'Portugal', TO_DATE('2022-11-05', 'YYYY-MM-DD'));
INSERT INTO Clients VALUES (5, 'Sophie Martin', '8 boulevard Haussmann, Paris', 'France', TO_DATE('2021-12-20', 'YYYY-MM-DD'));
INSERT INTO Clients VALUES (6, 'Jane Doe', '456 Elm St, Boston', 'USA', TO_DATE('2023-02-15', 'YYYY-MM-DD'));
INSERT INTO Clients VALUES (7, 'Victor Hugo', '12 rue Victor Hugo, Quebec', 'Canada', TO_DATE('2020-08-05', 'YYYY-MM-DD'));
INSERT INTO Clients VALUES (8, 'Liu Wei', '88 Peking Rd, Shanghai', 'China', TO_DATE('2021-09-30', 'YYYY-MM-DD'));
INSERT INTO Clients VALUES (9, 'Emma Watson', '100 Baker St, London', 'UK', TO_DATE('2022-03-12', 'YYYY-MM-DD'));
INSERT INTO Clients VALUES (10, 'Akira Yamada', '3 Chome Ginza, Tokyo', 'Japan', TO_DATE('2021-11-25', 'YYYY-MM-DD'));

COMMIT;

-- 3. Requêtes pour tester le partitionnement par lignes
-- Requête ciblant une partition spécifique (France)

SELECT * 
FROM Clients PARTITION (partition_france);


EXPLAIN PLAN FOR 
SELECT * 
FROM Clients PARTITION (partition_france);

SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);


-- Requête sur toutes les partitions
SELECT * 
FROM Clients
WHERE Pays IN ('USA', 'Canada');

--------------------------------------------------------

-- 4. Partitionnement par colonnes : Création des tables
-- Table des attributs statiques
CREATE TABLE Clients_Statique (
    Client_ID NUMBER PRIMARY KEY,
    Nom VARCHAR2(100),
    Date_Inscription DATE
);

-- Table des attributs dynamiques
CREATE TABLE Clients_Dynamique (
    Client_ID NUMBER PRIMARY KEY,
    Adresse VARCHAR2(255),
    Indice_Confiance NUMBER
);

-- 5. Insertion des données dans Clients_Statique
INSERT INTO Clients_Statique VALUES (1, 'Alice Dupont', TO_DATE('2022-01-10', 'YYYY-MM-DD'));
INSERT INTO Clients_Statique VALUES (2, 'John Smith', TO_DATE('2021-05-15', 'YYYY-MM-DD'));
INSERT INTO Clients_Statique VALUES (3, 'Marie Curie', TO_DATE('2023-03-20', 'YYYY-MM-DD'));
INSERT INTO Clients_Statique VALUES (4, 'Carlos Santos', TO_DATE('2022-11-05', 'YYYY-MM-DD'));
INSERT INTO Clients_Statique VALUES (5, 'Sophie Martin', TO_DATE('2021-12-20', 'YYYY-MM-DD'));
INSERT INTO Clients_Statique VALUES (6, 'Jane Doe', TO_DATE('2023-02-15', 'YYYY-MM-DD'));
INSERT INTO Clients_Statique VALUES (7, 'Victor Hugo', TO_DATE('2020-08-05', 'YYYY-MM-DD'));
INSERT INTO Clients_Statique VALUES (8, 'Liu Wei', TO_DATE('2021-09-30', 'YYYY-MM-DD'));
INSERT INTO Clients_Statique VALUES (9, 'Emma Watson', TO_DATE('2022-03-12', 'YYYY-MM-DD'));
INSERT INTO Clients_Statique VALUES (10, 'Akira Yamada', TO_DATE('2021-11-25', 'YYYY-MM-DD'));

COMMIT;

-- Insertion des données dans Clients_Dynamique
INSERT INTO Clients_Dynamique VALUES (1, '10 rue des Lilas, Paris', 85);
INSERT INTO Clients_Dynamique VALUES (2, '123 Main St, New York', 70);
INSERT INTO Clients_Dynamique VALUES (3, '5 avenue des Champs, Montreal', 90);
INSERT INTO Clients_Dynamique VALUES (4, '50 Rua de Lisboa', 60);
INSERT INTO Clients_Dynamique VALUES (5, '8 boulevard Haussmann, Paris', 88);
INSERT INTO Clients_Dynamique VALUES (6, '456 Elm St, Boston', 75);
INSERT INTO Clients_Dynamique VALUES (7, '12 rue Victor Hugo, Quebec', 95);
INSERT INTO Clients_Dynamique VALUES (8, '88 Peking Rd, Shanghai', 78);
INSERT INTO Clients_Dynamique VALUES (9, '100 Baker St, London', 65);
INSERT INTO Clients_Dynamique VALUES (10, '3 Chome Ginza, Tokyo', 82);

COMMIT;

-- 6. Création de la vue combinée pour simplifier les requêtes
CREATE OR REPLACE VIEW Clients_Complet AS
SELECT 
    S.Client_ID,
    S.Nom,
    S.Date_Inscription,
    D.Adresse,
    D.Indice_Confiance
FROM 
    Clients_Statique S
JOIN 
    Clients_Dynamique D ON S.Client_ID = D.Client_ID;

--------------------------------------------------------

-- 7. Requêtes pour tester le partitionnement par colonnes
-- Requête sur les données statiques uniquement
SELECT * 
FROM Clients_Statique
WHERE Date_Inscription > TO_DATE('2022-01-01', 'YYYY-MM-DD');

-- Requête sur les données dynamiques uniquement
SELECT * 
FROM Clients_Dynamique
WHERE Indice_Confiance > 80;

-- Requête sur la vue combinée
SELECT * 
FROM Clients_Complet
WHERE Indice_Confiance > 70 AND Date_Inscription > TO_DATE('2022-01-01', 'YYYY-MM-DD');


--------------------------------------------------------
-- Partitionnement hybride
--------------------------------------------------------

-- Cleanup

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE Clients CASCADE CONSTRAINTS';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE Clients_Statique CASCADE CONSTRAINTS';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE Clients_Dynamique CASCADE CONSTRAINTS';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP VIEW Clients_Complet';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/


-- 1. Création de la table des attributs statiques, partitionnée par pays
CREATE TABLE Clients_Statique (
    Client_ID NUMBER PRIMARY KEY,
    Nom VARCHAR2(100),
    Date_Inscription DATE,
    Pays VARCHAR2(50)
)
PARTITION BY LIST (Pays) (
    PARTITION partition_france VALUES ('France'),
    PARTITION partition_usa VALUES ('USA'),
    PARTITION partition_canada VALUES ('Canada'),
    PARTITION partition_autres VALUES (DEFAULT)
);

-- 2. Création de la table des attributs dynamiques, partitionnée par pays
CREATE TABLE Clients_Dynamique (
    Client_ID NUMBER PRIMARY KEY,
    Adresse VARCHAR2(255),
    Indice_Confiance NUMBER,
    Pays VARCHAR2(50)
)
PARTITION BY LIST (Pays) (
    PARTITION partition_france VALUES ('France'),
    PARTITION partition_usa VALUES ('USA'),
    PARTITION partition_canada VALUES ('Canada'),
    PARTITION partition_autres VALUES (DEFAULT)
);

-- 3. Insertion de données dans Clients_Statique
INSERT INTO Clients_Statique VALUES (1, 'Alice Dupont', TO_DATE('2022-01-10', 'YYYY-MM-DD'), 'France');
INSERT INTO Clients_Statique VALUES (2, 'John Smith', TO_DATE('2021-05-15', 'YYYY-MM-DD'), 'USA');
INSERT INTO Clients_Statique VALUES (3, 'Marie Curie', TO_DATE('2023-03-20', 'YYYY-MM-DD'), 'Canada');
INSERT INTO Clients_Statique VALUES (4, 'Carlos Santos', TO_DATE('2022-11-05', 'YYYY-MM-DD'), 'Portugal');
INSERT INTO Clients_Statique VALUES (5, 'Sophie Martin', TO_DATE('2021-12-20', 'YYYY-MM-DD'), 'France');

COMMIT;

-- 4. Insertion de données dans Clients_Dynamique
INSERT INTO Clients_Dynamique VALUES (1, '10 rue des Lilas, Paris', 85, 'France');
INSERT INTO Clients_Dynamique VALUES (2, '123 Main St, New York', 70, 'USA');
INSERT INTO Clients_Dynamique VALUES (3, '5 avenue des Champs, Montreal', 90, 'Canada');
INSERT INTO Clients_Dynamique VALUES (4, '50 Rua de Lisboa', 60, 'Portugal');
INSERT INTO Clients_Dynamique VALUES (5, '8 boulevard Haussmann, Paris', 88, 'France');

COMMIT;

-- 5. Création de la vue combinée pour simplifier les requêtes
CREATE OR REPLACE VIEW Clients_Complet AS
SELECT 
    S.Client_ID,
    S.Nom,
    S.Date_Inscription,
    D.Adresse,
    D.Indice_Confiance,
    S.Pays
FROM 
    Clients_Statique S
JOIN 
    Clients_Dynamique D ON S.Client_ID = D.Client_ID ;

-- 6. Requêtes pour tester le partitionnement hybride

-- Requête 1 : Cibler les attributs statiques uniquement (France)
SELECT * 
FROM Clients_Statique PARTITION (partition_france)
WHERE Date_Inscription > TO_DATE('2021-01-01', 'YYYY-MM-DD');

-- Requête 2 : Cibler les attributs dynamiques uniquement (USA)
SELECT * 
FROM Clients_Dynamique PARTITION (partition_usa)
WHERE Indice_Confiance > 70;

-- Requête 3 : Requête combinée via la vue
SELECT * 
FROM Clients_Complet
WHERE Pays = 'Canada' AND Indice_Confiance > 80 AND Date_Inscription > TO_DATE('2022-01-01', 'YYYY-MM-DD');

-- Requête 4 : Requête globale sur tous les pays pour les clients avec un indice de confiance > 85
SELECT * 
FROM Clients_Complet
WHERE Indice_Confiance > 85;
