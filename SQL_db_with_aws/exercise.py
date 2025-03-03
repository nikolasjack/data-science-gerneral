# installing pymysql if missing
# !pip install pymysql
import pandas as pd
import pymysql as sql
from sqlalchemy import create_engine
import time
from dotenv import load_dotenv
import os

user = os.getenv("USER")
pw = os.getenv("PASSWORD")
address = os.getenv("ADDRESS")
port = 3306
db = "Chinook"

# Build engine and connection
engine = create_engine(f"mysql+pymysql://{user}:{pw}@{address}:{port}/{db}")
con = engine.connect()

#Initial inspection
from sqlalchemy import inspect
inspector = inspect(engine)

print(inspector.get_table_names())

#A complete look
query = "SELECT * FROM Track"
result = pd.read_sql(query, engine)
print(result)


#Showing specific customer from Germany
query = "SELECT * FROM Customer WHERE Country = 'Germany'"

result = pd.read_sql(query, engine)

print(result)

# Show all invoices that have been issues between Dec 2011 and March 2012.

query = "SELECT * FROM Invoice WHERE InvoiceDate BETWEEN '2011-12-01' AND '2012-03-31'"

result = pd.read_sql(query, engine)

print(result)

#Show the Title, FirstName, LastName, City, PostalCode of all employees whose PostalCode starts with T2P

query = "Select LastName, FirstName, Title, City, PostalCode FROM Employee WHERE PostalCode LIKE 'T2%' "

result = pd.read_sql(query, engine)

print(result)

#Grouping

#Group all tracks and get the average unit price

query = "SELECT AVG(UnitPrice) AS AverageUnitPrice FROM Track"

result = pd.read_sql(query, engine)

print(result)

#Count all employees that directly report to employee number 6.

query = "SELECT COUNT(*) AS NumEmployees FROM Employee WHERE ReportsTo = 6"

result = pd.read_sql(query, engine)

print(result)

#Group all albums by artist and display only those ArtistIds that have more than 3 albums.

query = "SELECT ArtistId, COUNT(*) AS NumAlbums FROM Album GROUP BY ArtistId HAVING COUNT(*) > 3"

result = pd.read_sql(query, engine)

print(result)

#JOINs

# Join the tracks and genre together to get more information on the genre name for each song

query = """SELECT * FROM Track TK JOIN Genre GR On GR.Genreid = TK.Genreid """
result = pd.read_sql(query, engine)

print(result)

# Join the media_type table as well

query = """
SELECT Track.Name, Genre.Name AS GenreName
FROM Track
JOIN Genre ON Track.GenreId = Genre.GenreId
"""

result = pd.read_sql(query, engine)

print(result)

# Show only the different Mediatype names used for the Genres Classical and Opera.

query = """
SELECT DISTINCT MediaType.Name
FROM MediaType
JOIN Track ON MediaType.MediaTypeId = Track.MediaTypeId
JOIN Genre ON Track.GenreId = Genre.GenreId
WHERE Genre.Name IN ('Classical', 'Opera')
"""

result = pd.read_sql(query, engine)

print(result)

# Show the Names of all Artists that have made Tracks over 15 minutes. Show each name only once!

query = """
SELECT DISTINCT Artist.Name
FROM Artist
JOIN Album ON Artist.ArtistId = Album.ArtistId
JOIN Track ON Album.AlbumId = Track.AlbumId
WHERE Track.Milliseconds > 900000
"""

result = pd.read_sql(query, engine)

print(result)

# Make a table of each combination of CustomerLastName, CustomerFirstName, CustomerCompany and the responsible EmployeeFirstName, EmployeeFirstName.

query = """
SELECT 
    e.FirstName AS EmployeeFirstName,
    e.LastName AS EmployeeLastName,
    c.FirstName AS CustomerFirstName,
    c.LastName AS CustomerLastName
    FROM Employee e LEFT JOIN Customer c ON e.EmployeeId = c.SupportRepId
"""

result = pd.read_sql(query, engine)

print(result)

# Subqueries with WHERE

# Get the Title, FirstName, LastName of all Employees that are responsible for customers in the USA.

query = """
SELECT 
    e.FirstName,
    e.LastName,
    c.Country
FROM Employee e, (
    SELECT SupportRepId,Country
        FROM Customer WHERE Country = 'USA') c WHERE e.EmployeeId = c.SupportRepId
"""

result = pd.read_sql(query, engine)

print(result)

# Get the artist name (not Composer) of the song "L.A. Is My Lady"

query = """
SELECT *
FROM artists
WHERE ArtistId IN ( SELECT ArtistId FROM albums WHERE AlbumId IN (SELECT AlbumId FROM tracks WHERE Name = 'L.A. Is My Lady')
)"""

print(result)

#Show all customer's FirstName, LastName, Company that have had a maximum invoice of 5$ or higher in 2013.

query = """
SELECT FirstName, LastName, Company FROM Customer, Invoice
WHERE Customer.CustomerId = Invoice.CustomerId AND Total > 5.00  AND InvoiceDate BETWEEN '2013-01-01' AND '2013-12-31'
GROUP BY Total, InvoiceDate;

"""



result = pd.read_sql(query, engine)

print(result)

# Count, how many Reggae Tracks over 4 minutes long each album has and display only those Album titles with more than 6 of such songs.

query = """
SELECT Album.Title
FROM Album
WHERE Album.AlbumId IN (
    SELECT Track.AlbumId FROM Track
    WHERE Track.Milliseconds > 240000 AND Track.GenreId = ( SELECT Genre.GenreId FROM Genre WHERE Genre.Name = 'Reggae')
GROUP BY Track.AlbumId HAVING COUNT(*) > 6 )
"""

result = pd.read_sql(query, engine)

print(result)

#Mixed Queries

#Group the table playlist_track by PlaylistId to count how many songs are in each Playlist. Join the result with the Playlist table to have the names as well instead of the PlaylistId.

query = """
SELECT p.PlaylistId, p.Name AS PlaylistName, COUNT(pt.PlaylistId) AS NumOfSongs
FROM Playlist AS p
INNER JOIN PlaylistTrack AS pt ON p.PlaylistId = pt.PlaylistId
GROUP BY p.PlaylistId

"""



result = pd.read_sql(query, engine)

print(result)

#How much revenue did the company generate in each country through invoices? List the Name of each Country and its Revenue.

query = """
SELECT BillingCountry AS Country, SUM(Total) AS Revenue
FROM Invoice
GROUP BY Country

"""



result = pd.read_sql(query, engine)

print(result)

#Which Tracks from which Albums (give TrackName and AlbumTitle) were bought by Astrid Gruber? Order by AlbumTitle!

query = """
SELECT t.Name AS TrackName, a.Title AS AlbumTitle
FROM Invoice AS i
JOIN InvoiceLine AS il ON i.InvoiceId = il.InvoiceId
JOIN Track AS t ON il.TrackId = t.TrackId
JOIN Album AS a ON t.AlbumId = a.AlbumId
JOIN Customer AS c ON i.CustomerId = c.CustomerId
WHERE c.FirstName = 'Astrid' AND c.LastName = 'Gruber'
ORDER BY a.Title

"""



result = pd.read_sql(query, engine)

print(result)