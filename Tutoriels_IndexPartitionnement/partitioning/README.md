
# Script Oracle : Partitionnement des Données

Ce script explore les différentes méthodes de partitionnement dans Oracle, notamment :
- **Partitionnement par lignes** : Organisation des données en fonction des valeurs spécifiques d'une colonne.
- **Partitionnement par colonnes** : Division logique des données en tables distinctes.
- **Partitionnement hybride** : Combinaison des deux approches précédentes.

Chaque section du script est accompagnée d'exemples pour tester et comprendre le partitionnement.

---

Petit néttoyage de la base au cas où vous exécutiez ce script plusieurs fois.

```sql
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
```

Ready to go.

---

## 1. Partitionnement par Lignes 

Le partitionnement par lignes consiste à séparer les données en partitions basées sur des valeurs spécifiques.

### Création de la Table Clients
```sql
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
```

Vous pouvez consulter les autres options de partitionnement [ici](https://docs.oracle.com/en/database/oracle/oracle-database/19/vldbg/partition-create-tables-indexes.html?utm_source=chatgpt.com#GUID-0CAB4231-E7DB-4245-9C43-C9CA352EC298)

### Insertion des Données
```sql
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
```

### Requêtes de Test
- Requête ciblant une partition spécifique (`partition_france`) :
  ```sql
  SELECT * 
  FROM Clients PARTITION (partition_france);
  ```
- Requête sur plusieurs partitions :
  ```sql
  SELECT * 
  FROM Clients
  WHERE Pays IN ('USA', 'Canada');
  ```

- Que se passe-t-il dans l'optimiseur ?
  ```sql
  EXPLAIN PLAN FOR
  SELECT * 
  FROM Clients PARTITION (partition_france);
  
  SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);

  EXPLAIN PLAN FOR
  SELECT * 
  FROM Clients
  WHERE Pays IN ('USA', 'Canada');

  SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);
  ```


---

## 2. Partitionnement par Colonnes

Le partitionnement par colonnes divise les données en plusieurs tables logiques selon leur nature (statique ou dynamique).

### Création des Tables
#### Table des Attributs Statiques
```sql
CREATE TABLE Clients_Statique (
    Client_ID NUMBER PRIMARY KEY,
    Nom VARCHAR2(100),
    Date_Inscription DATE
);
```

#### Table des Attributs Dynamiques
```sql
CREATE TABLE Clients_Dynamique (
    Client_ID NUMBER PRIMARY KEY,
    Adresse VARCHAR2(255),
    Indice_Confiance NUMBER
);
```

### Insertion des Données
```sql
-- Insertion des données dans Clients_Statique
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
```

### Requêtes de Test
- Requête sur les données statiques uniquement :
  ```sql
  SELECT * 
  FROM Clients_Statique
  WHERE Date_Inscription > TO_DATE('2022-01-01', 'YYYY-MM-DD');
  ```
- Requête sur les données dynamiques uniquement :
  ```sql
  SELECT * 
  FROM Clients_Dynamique
  WHERE Indice_Confiance > 80;
  ```

  Que se passe-t-il dans l'optimiseur ?
   ```sql
  EXPLAIN PLAN FOR
  SELECT * 
  FROM Clients_Statique
  WHERE Date_Inscription > TO_DATE('2022-01-01', 'YYYY-MM-DD');
  
  SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);

  EXPLAIN PLAN FOR
  SELECT * 
  FROM Clients_Dynamique
  WHERE Indice_Confiance > 80;
  
  SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);
  ```

---

## 3. Partitionnement Hybride (Lignes + Colonnes)

Nous allons devoir d'abord nettoyer la base.

```sql
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
```

### Création des Tables Partitionnées
#### Clients_Statique Partitionné
```sql
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
```

#### Clients_Dynamique Partitionné
```sql
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
```

Ajoutons quelques données.

```sql
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
```


### Requêtes de Test
1. **Cibler une partition spécifique :**
   ```sql
   SELECT * 
   FROM Clients_Statique PARTITION (partition_france)
   WHERE Date_Inscription > TO_DATE('2021-01-01', 'YYYY-MM-DD');
   ```

2. **Requête combinée via une vue :**
   ```sql
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

   SELECT * 
   FROM Clients_Complet
   WHERE Indice_Confiance > 80 AND Date_Inscription > TO_DATE('2022-01-01', 'YYYY-MM-DD');
   ```

Que se passe-t-il dans l'optimiseur ?

```sql
   EXPLAIN PLAN FOR
   SELECT * 
   FROM Clients_Statique PARTITION (partition_france)
   WHERE Date_Inscription > TO_DATE('2021-01-01', 'YYYY-MM-DD');

   SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);

   EXPLAIN PLAN FOR
   SELECT * 
   FROM Clients_Complet
   WHERE Indice_Confiance > 80 AND Date_Inscription > TO_DATE('2022-01-01', 'YYYY-MM-DD');

   SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);
   ```

---

## Conclusion

Ce script vous permet de découvrir et d'explorer :
- Le partitionnement par listes pour organiser les données en fonction des valeurs spécifiques.
- Le partitionnement par colonnes pour diviser logiquement les données.
- La combinaison de ces deux approches pour optimiser les requêtes.

Utilisez les exemples de requêtes pour analyser les performances et comprendre comment Oracle gère ces partitions.
