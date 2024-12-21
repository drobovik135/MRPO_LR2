CREATE TABLE Providers (
    ID SERIAL PRIMARY KEY,
    Name VARCHAR(255) NOT NULL,
    Email VARCHAR(255),
    ContactPerson VARCHAR(255),
    Comments TEXT
);

CREATE TABLE ProcurementStatuses (
    ID SERIAL PRIMARY KEY,
    Name VARCHAR(255) NOT NULL
);

CREATE TABLE SouvenirCategories (
    ID SERIAL PRIMARY KEY,
    IdParent INT,
    Name VARCHAR(255) NOT NULL,
    FOREIGN KEY (IdParent) REFERENCES SouvenirCategories(ID)
);

CREATE TABLE Colors (
    ID SERIAL PRIMARY KEY,
    Name VARCHAR(255) NOT NULL
);

CREATE TABLE SouvenirMaterials (
    ID SERIAL PRIMARY KEY,
    Name VARCHAR(255) NOT NULL
);

CREATE TABLE ApplicationMethods (
    ID SERIAL PRIMARY KEY,
    Name VARCHAR(255) NOT NULL
);

CREATE TABLE Souvenirs (
    ID SERIAL PRIMARY KEY,
    ShortName VARCHAR(255),
    Name VARCHAR(255) NOT NULL,
    Description TEXT,
    Rating INT,
    IdCategory INT,
    IdColor INT,
    Size TEXT,
    IdMaterial INT,
    Weight DECIMAL,
    QTopics INT,
    PicsSize VARCHAR(255),
    IdApplicMethod INT,
    AllCategories TEXT,
    DealerPrice DECIMAL,
    Price FLOAT,
    Comments TEXT,
    FOREIGN KEY (IdCategory) REFERENCES SouvenirCategories(ID),
    FOREIGN KEY (IdColor) REFERENCES Colors(ID),
    FOREIGN KEY (IdMaterial) REFERENCES SouvenirMaterials(ID),
    FOREIGN KEY (IdApplicMethod) REFERENCES ApplicationMethods(ID)
);

CREATE TABLE SouvenirProcurements (
    ID SERIAL PRIMARY KEY,
    IdProvider INT,
    Data DATE,
    IdStatus INT,
    FOREIGN KEY (IdProvider) REFERENCES Providers(ID),
    FOREIGN KEY (IdStatus) REFERENCES ProcurementStatuses(ID)
);

CREATE TABLE ProcurementSouvenirs (
    ID SERIAL PRIMARY KEY,
    IdSouvenir INT,
    IdProcurement INT,
    Amount INT,
    Price DECIMAL,
    FOREIGN KEY (IdSouvenir) REFERENCES Souvenirs(ID),
    FOREIGN KEY (IdProcurement) REFERENCES SouvenirProcurements(ID)
);

CREATE TABLE SouvenirStores (
    ID SERIAL PRIMARY KEY,
    IdProcurement INT,
    IdSouvenir INT,
    Amount INT,
    Comments TEXT,
    FOREIGN KEY (IdProcurement) REFERENCES SouvenirProcurements(ID),
    FOREIGN KEY (IdSouvenir) REFERENCES Souvenirs(ID)
);

SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public';




