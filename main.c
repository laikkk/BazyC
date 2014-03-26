/* 
 * File:   main.c
 * Author: Kamil Zieliński
 * 
 *      Dołączyłem też plik makefile
 *      sposob uruchomienia: make makefile program
 *      nastepnie:              ./program nazwa_bazy nazwa_tabeli1 nazwa_tabeli2 .. > strona.html lub
 *                              ./program nazwa_pliku.csv
 *      kompilacja z ręki:
 *      gcc -o main main.c -I/usr/include/postgresql -L/usr/lib -lpq
 * 
 * na innym komputerze mogą wysątpić problemy z plikami bibliotek. Na slajdzie były:
 * -I/usr/include/postgresql -L/usr/local/pgsql/lib -lpq
 * Created on 16 marzec 2014, 10:23
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <libpq-fe.h>
#define CONN_TEXT "host=localhost port=5432 dbname=kamil user=kamil password=1234"
#define DL_NAZWY_PLIKU 128
#define DL_ZAPYTANIA 512

// DANE KONFIGURACYJNE 
char HOST[DL_ZAPYTANIA] = "localhost";
char PORT[DL_ZAPYTANIA] = "5432";
char DBNAME[DL_ZAPYTANIA] = "kamil";
char USER[DL_ZAPYTANIA] = "kamil";
char PASSWORD[DL_ZAPYTANIA] = "1234";
char CONNECTION_STRING[DL_ZAPYTANIA] = "";

typedef struct
{
    char nazwa_kolumny[DL_NAZWY_PLIKU];
    int dlugosc_pola;
} Struktura;

void CreateConnectionString()
{//'skleja' stringa do laczenia z baza danych. MUSI być wywolane przed operacjami na bazie
    sprintf(CONNECTION_STRING, "host=%s port=%s dbname=%s user=%s password=%s", HOST, PORT, DBNAME, USER, PASSWORD);
}

PGconn* ConectToDatebase()
{
    CreateConnectionString();
    PGconn *myconnection = PQconnectdb(CONNECTION_STRING);
    if (PQstatus(myconnection)==CONNECTION_OK)
    {
        //printf("Udało sie połączyć z bazą\n");
        return myconnection;
    }
    else
    {
        printf("nie moglem polaczyc sie do bazy !\n");
        exit(1);
    }
}

int CountColumnt(char* nazwaPliku) // zlicza ilosc kolumn w pliku csv
{
    FILE *plik;
    if ((plik = fopen(nazwaPliku, "r"))==NULL)//argv[1]
    {
        printf("Nie mogę otworzyć pliku %s\n", nazwaPliku);
        return 1;
    }
    char pierwszy[512];
    fgets(pierwszy, 511, plik);
    int i = 0, licznik_kolumn = 0;
    while (pierwszy[i]!='\0')
    {
        if (pierwszy[i]==';') licznik_kolumn++;
        i++;
    }
    if (licznik_kolumn==0)
    {
        printf("Prawdopodobnie bledy w pliku lub zero kolumn\n");
        exit(1);
    }
    fclose(plik);
    return licznik_kolumn+1; //+1 bo ostania kolumna w pliku csv nie ma srednika

}

void prepareCreatestmt(char* nazwaPliku, char* nazwaTabeli, char* query, Struktura *struktura)//przygotowuje polecenie CREATE TABLE. Do argumentu query zostaje wpisany zapytanie SQL
{
    FILE *plik;
    if ((plik = fopen(nazwaPliku, "r"))==NULL)//argv[1]
    {
        printf("Nie mogę otworzyć pliku %s\n", nazwaPliku);
        exit(1);
    }
    char pierwszy[DL_ZAPYTANIA], drugi[DL_ZAPYTANIA];
    fgets(pierwszy, DL_ZAPYTANIA, plik); //Pobieramy pierwsza linijke z pliku.csv
    strcpy(drugi, pierwszy);
    char *result = NULL;
    result = strtok(pierwszy, ";");
    int licznikKolumn = 0;
    while (result!=NULL)
    {
        struktura[licznikKolumn].dlugosc_pola = 30;
        strcpy(struktura[licznikKolumn].nazwa_kolumny, result);
        result = strtok(NULL, ";");
        licznikKolumn++;
    }
    if (licznikKolumn>0)
    {
        char Kolumny[licznikKolumn][DL_ZAPYTANIA];
        result = strtok(drugi, ";");
        licznikKolumn = 0;
        while (result!=NULL)
        {
            strcpy(Kolumny[licznikKolumn], result);
            result = strtok(NULL, ";");
            licznikKolumn++;
        }

        sprintf(query, "CREATE TABLE %s(", nazwaTabeli);
        int x;
        for (x = 0; x<licznikKolumn; x++)
        {
            strcat(query, Kolumny[x]);
            if (x==0)
                strcat(query, " VARCHAR(30) UNIQUE,");
            else if (x+1==licznikKolumn)
                strcat(query, " VARCHAR(30) );");
            else strcat(query, " VARCHAR(30), ");
        }
        if (licznikKolumn==0)
        {
            printf("Prawdopodobnie bledy w pliku lub zero kolumn\n");
            exit(1);
        }
    }
}

int IfTableExist(char* nazwaTabeli, PGconn* conn)//sprawdza czy Tabela istnieje w bazie
{
    char query[DL_ZAPYTANIA];
    sprintf(query, "SELECT 1 FROM pg_tables WHERE tablename ='%s'", nazwaTabeli);
    PGresult *result = PQexec(conn, query);
    return PQntuples(result);
}

void Msg(char* tresc, PGresult *result)//powiadamia o sukcesie/porazce operacji
{
    ExecStatusType status = PQresultStatus(result);
    switch (status)
    {
    case PGRES_COMMAND_OK: printf("Udało sie wykonac polecenie\t %s\n", tresc);
        break;
    case PGRES_TUPLES_OK: printf("Udało sie wykonac polecenie\t %s\n", tresc);
        break;
    case PGRES_NONFATAL_ERROR: printf("NIE udało sie wykonac polecenia\t %s\n", tresc);
        printf("BLAD  STATUS:\t %s\n", PQresStatus(status));
        printf("BLAD \n\t %s\n", PQresultErrorMessage(result));
        break;
    case PGRES_FATAL_ERROR: printf("FATAL ERROR \t %s\n", tresc);
        printf("BLAD  STATUS:\t %s\n", PQresStatus(status));
        printf("BLAD \n\t %s\n", PQresultErrorMessage(result));
        break;
    }
}

void ChangeSizeofColumn(Struktura *struktura, int pozycja, char* nazwaTabeli, PGconn* myconnection)//zmienia wielkosc kolumny
{
    char query[DL_ZAPYTANIA] = "";
    sprintf(query, "ALTER TABLE %s ALTER COLUMN %s TYPE varchar(%d);", nazwaTabeli, struktura[pozycja].nazwa_kolumny, struktura[pozycja].dlugosc_pola += 30);
    PGresult *result = PQexec(myconnection, query);
    Msg("Zmiana dlugosci tabeli", result);
    PQclear(result);
}

void InsertRow(PGconn* myconnection, char* NazwaPliku, FILE* plik, char* NazwaTabeli, Struktura *struktura)
{
    char Rekordy[CountColumnt(NazwaPliku)][DL_NAZWY_PLIKU];
    char slowo[DL_NAZWY_PLIKU];
    int c, i, firstTime = 0, doinsert = 0;
    while ((c = (int)fgets(slowo, DL_NAZWY_PLIKU, plik))!=0) //bierze 99znakow  dopisuje '\0' na koncu i zapisuje do zmiennej slowo
    {
        int x = 0, k = 0, j = 0, n, kolumna = 0;
        do
        {
            if (firstTime==0) //odrzucenie pierwszego wiersza
            {
                firstTime = 1;
                break;
            }
            char wyraz[100] = "";
            while (slowo[x]!='\0'&&slowo[x]!=';') x++;
            if (slowo[x]=='\0') break;
            j = k;
            k = 0;
            n = 0;
            for (k = j; k<x; k++)
                wyraz[n++] = slowo[k];
            wyraz[k] = '\0';
            strcpy(Rekordy[kolumna++], wyraz);
            x += 2; //omijamy sredniki
            k++; //omijamy sredniki
        }
        while (1);

        if (doinsert==0) doinsert = 1;
        else
        {
            char insertQuery[1000] = "";
            sprintf(insertQuery, "INSERT INTO %s VALUES (", NazwaTabeli);
            for (i = 0; i<kolumna; i++)
            {
                char prepare[100] = "\'";
                if (strlen(Rekordy[i])>struktura[i].dlugosc_pola)
                    ChangeSizeofColumn(struktura, i, NazwaTabeli, myconnection);
                strcat(prepare, Rekordy[i]);
                strcat(prepare, "\'");
                strcat(insertQuery, prepare);
                if (i+1!=kolumna)strcat(insertQuery, ", ");
            }
            strcat(insertQuery, "); ");
            PGresult *result = PQexec(myconnection, insertQuery);
            Msg("Dodałem rekord", result);
            PQclear(result);
        }
    }
}

void newInsert(PGconn* myconnection, char* NazwaPliku, FILE* plik, char* NazwaTabeli, Struktura *struktura) //dodaje rekordy do bazy
{
    char pierwszy[DL_ZAPYTANIA];
    fgets(pierwszy, DL_ZAPYTANIA, plik); //omijamy tym pierwsza linijke

    int licznikKolumn, i, c, iloscColumn = CountColumnt(NazwaPliku);
    while ((c = (int)fgets(pierwszy, DL_ZAPYTANIA, plik))!=0)
    {
        licznikKolumn = 0;
        char *result = NULL;
        result = strtok(pierwszy, ";");
        char Kolumny[iloscColumn][DL_NAZWY_PLIKU];
        while (result!=NULL)
        {
            strcpy(Kolumny[licznikKolumn], result);
            result = strtok(NULL, ";");
            licznikKolumn++;
        }
        char insertQuery[1000] = "";
        sprintf(insertQuery, "INSERT INTO %s VALUES (", NazwaTabeli);
        for (i = 0; i<licznikKolumn; i++)
        {
            char prepare[100] = "\'";
            if (strlen(Kolumny[i])>struktura[i].dlugosc_pola)
                ChangeSizeofColumn(struktura, i, NazwaTabeli, myconnection);
            strcat(prepare, Kolumny[i]);
            strcat(prepare, "\'");
            strcat(insertQuery, prepare);
            if (i+1!=licznikKolumn)strcat(insertQuery, ", ");
        }
        strcat(insertQuery, "); ");
        PGresult *resultt = PQexec(myconnection, insertQuery);
        Msg("Dodałem rekord", resultt);
        PQclear(resultt);
    }
}

void Select(PGconn* myconnection, char* Nazwatabeli)
{
    char select[DL_ZAPYTANIA] = "";
    sprintf(select, "SELECT * FROM %s", Nazwatabeli);
    PGresult *result = PQexec(myconnection, select);
    ExecStatusType status = PQresultStatus(result);
    if (status==PGRES_TUPLES_OK)
    {
        printf("<div class=\"Tablestyle\"><table><caption>%s</caption>", Nazwatabeli);
        int iloscKolumn = PQnfields(result), iloscWierszy = PQntuples(result), i, j;
        printf("<tr>");
        for (j = 0; j<iloscKolumn; j++)
            printf("<td>%s</td>", PQfname(result, j));
        printf("</tr>");
        for (i = 0; i<iloscWierszy; i++)
        {
            printf("<tr>");
            for (j = 0; j<iloscKolumn; j++)
                printf("<td>%s</td>", PQgetvalue(result, i, j));
            printf("</tr>");
        }
        printf("</table></div>");
    }
    PQclear(result);
}

int main(int argc, char** argv)
{
    if (argc==2)
    {
        //dodajemy tabele do bazy;
        PGconn *myconnection = ConectToDatebase();
        char nazwa[DL_NAZWY_PLIKU];
        char *format;
        int iloscKolumn = CountColumnt(argv[1]);
        FILE *plik;
        Struktura Struk[iloscKolumn];

        strcpy(nazwa, argv[1]);
        strtok_r(nazwa, ".", &format);

        if ((plik = fopen(argv[1], "r"))==NULL)
        {
            printf("Nie mogę otworzyć pliku %s\n", argv[1]);
            exit(1);
        }
        if (IfTableExist(nazwa, myconnection))
        {
            char DropQuery[DL_ZAPYTANIA];
            sprintf(DropQuery, "DROP TABLE %s", nazwa);
            PGresult *result = PQexec(myconnection, DropQuery);
            Msg(DropQuery, result);
            PQclear(result);
        }

        char query[DL_ZAPYTANIA] = "";
        prepareCreatestmt(argv[1], nazwa, query, Struk);
        PGresult *result = PQexec(myconnection, query);
        Msg("CREATE TABLE", result);
        PQclear(result);

        /*
         int i;
                for (i = 0; i<iloscKolumn; i++)
                    printf("\t%s (%d)\n", Struk[i].nazwa_kolumny, Struk[i].dlugosc_pola);
         */

        newInsert(myconnection, argv[1], plik, nazwa, Struk);
        fclose(plik);
        PQfinish(myconnection);
    }
    else if (argc>2)
    {
        //robimy plik index.html
        strcpy(DBNAME, argv[1]);
        PGconn *myconnection = ConectToDatebase();

        // <editor-fold defaultstate="collapsed" desc="Naglowek i style pliku html">
        printf("<!DOCTYPE HTML> \
<html>  \
<head>  \
  \
<meta charset= \"UTF-8\"/> \
 \
 <meta name= \"author\" content= \"Kamil Zieliński\"/> \
 <meta name= \"Robots\" content= \"all\" /> \
 <title>APLIKACJE BAZODANOWE - KAMIL ZIELIŃSKI</title> \
\
<style type=\"text/css\">\
     body{");

        printf("background: #e4f5fc no-repeat center center fixed; /* Old browsers */");
        printf("/* IE9 SVG, needs conditional override of 'filter' to 'none' */");
        printf("background: url(data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiA/Pgo8c3ZnIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgd2lkdGg9IjEwMCUiIGhlaWdodD0iMTAwJSIgdmlld0JveD0iMCAwIDEgMSIgcHJlc2VydmVBc3BlY3RSYXRpbz0ibm9uZSI+CiAgPGxpbmVhckdyYWRpZW50IGlkPSJncmFkLXVjZ2ctZ2VuZXJhdGVkIiBncmFkaWVudFVuaXRzPSJ1c2VyU3BhY2VPblVzZSIgeDE9IjAlIiB5MT0iMCUiIHgyPSIxMDAlIiB5Mj0iMTAwJSI+CiAgICA8c3RvcCBvZmZzZXQ9IjAlIiBzdG9wLWNvbG9yPSIjZTRmNWZjIiBzdG9wLW9wYWNpdHk9IjEiLz4KICAgIDxzdG9wIG9mZnNldD0iNTIlIiBzdG9wLWNvbG9yPSIjOWZkOGVmIiBzdG9wLW9wYWNpdHk9IjEiLz4KICAgIDxzdG9wIG9mZnNldD0iMTAwJSIgc3RvcC1jb2xvcj0iIzJhYjBlZCIgc3RvcC1vcGFjaXR5PSIxIi8+CiAgPC9saW5lYXJHcmFkaWVudD4KICA8cmVjdCB4PSIwIiB5PSIwIiB3aWR0aD0iMSIgaGVpZ2h0PSIxIiBmaWxsPSJ1cmwoI2dyYWQtdWNnZy1nZW5lcmF0ZWQpIiAvPgo8L3N2Zz4=);");
        printf("background: -moz-linear-gradient(-45deg,  #e4f5fc 0%%, #9fd8ef 52%%, #2ab0ed 100%%) no-repeat center center fixed; /* FF3.6+ */");
        printf("background: -webkit-gradient(linear, left top, right bottom, color-stop(0%%,#e4f5fc), color-stop(52%%,#9fd8ef), color-stop(100%%,#2ab0ed)) no-repeat center center fixed; /* Chrome,Safari4+ */");
        printf("background: -webkit-linear-gradient(-45deg,  #e4f5fc 0%%,#9fd8ef 52%%,#2ab0ed 100%%) no-repeat center center fixed; /* Chrome10+,Safari5.1+ */");
        printf("background: -o-linear-gradient(-45deg,  #e4f5fc 0%%,#9fd8ef 52%%,#2ab0ed 100%%) no-repeat center center fixed; /* Opera 11.10+ */");
        printf("background: -ms-linear-gradient(-45deg,  #e4f5fc 0%%,#9fd8ef 52%%,#2ab0ed 100%%) no-repeat center center fixed; /* IE10+ */");
        printf("background: linear-gradient(135deg,  #e4f5fc 0%%,#9fd8ef 52%%,#2ab0ed 100%%) no-repeat center center fixed; /* W3C */");
        printf("filter: progid:DXImageTransform.Microsoft.gradient( startColorstr='#e4f5fc', endColorstr='#2ab0ed',GradientType=1 ); /* IE6-8 fallback on horizontal gradient */");

        printf("width:960px;\
margin:auto;\
\
}\
.Tablestyle {\
	margin:20px;padding:0px;\
	\
	box-shadow: 10px 10px 5px #888888;\
	border:1px solid #000000;\
	\
	-moz-border-radius-bottomleft:14px;\
	-webkit-border-bottom-left-radius:14px;\
	border-bottom-left-radius:14px;\
	\
	-moz-border-radius-bottomright:14px;\
	-webkit-border-bottom-right-radius:14px;\
	border-bottom-right-radius:14px;\
	\
	-moz-border-radius-topright:14px;\
	-webkit-border-top-right-radius:14px;\
	border-top-right-radius:14px;\
	\
	-moz-border-radius-topleft:14px;\
	-webkit-border-top-left-radius:14px;\
	border-top-left-radius:14px;\
}.Tablestyle table{\
    border-collapse: collapse;\
        border-spacing: 0;\
	width:100%%;\
	height:100%%;\
	margin:0px;padding:0px;\
}.Tablestyle tr:last-child td:last-child {\
	-moz-border-radius-bottomright:14px;\
	-webkit-border-bottom-right-radius:14px;\
	border-bottom-right-radius:14px;\
}\
.Tablestyle table tr:first-child td:first-child {\
	-moz-border-radius-topleft:14px;\
	-webkit-border-top-left-radius:14px;\
	border-top-left-radius:14px;\
}\
.Tablestyle table tr:first-child td:last-child {\
	-moz-border-radius-topright:14px;\
	-webkit-border-top-right-radius:14px;\
	border-top-right-radius:14px;\
}.Tablestyle tr:last-child td:first-child{\
	-moz-border-radius-bottomleft:14px;\
	-webkit-border-bottom-left-radius:14px;\
	border-bottom-left-radius:14px;\
}.Tablestyle tr:hover td{\
	background-color:#ffffff;} ");
        printf("\
.Tablestyle td{\
	vertical-align:middle;\
	background-color:#aad4ff;\
	border:1px solid #000000;\
	border-width:0px 1px 1px 0px;\
	text-align:left;\
	padding:7px;\
	font-size:10px;\
	font-family:Arial;\
	font-weight:normal;\
	color:#000000;\
}.Tablestyle tr:last-child td{\
	border-width:0px 1px 0px 0px;\
}.Tablestyle tr td:last-child{\
	border-width:0px 0px 1px 0px;\
}.Tablestyle tr:last-child td:last-child{\
	border-width:0px 0px 0px 0px;\
}\
.Tablestyle tr:first-child td{\
	background:-o-linear-gradient(bottom, #005fbf 5%%, #003f7f 100%%);\
        background:-webkit-gradient( linear, left top, left bottom, color-stop(0.05, #005fbf), color-stop(1, #003f7f) );\
	background:-moz-linear-gradient( center top, #005fbf 5%%, #003f7f 100%% );\
	filter:progid:DXImageTransform.Microsoft.gradient(startColorstr=\"#005fbf\", endColorstr=\"#003f7f\");\
  	background: -o-linear-gradient(top,#005fbf,003f7f);\
	background-color:#005fbf;\
	border:0px solid #000000;\
	text-align:center;\
	border-width:0px 0px 1px 1px;\
	font-size:14px;\
	font-family:Arial;\
	font-weight:bold;\
	color:#ffffff;\
}\
.Tablestyle tr:first-child:hover td{\
	background:-o-linear-gradient(bottom, #005fbf 5%%, #003f7f 100%%);\
        background:-webkit-gradient( linear, left top, left bottom, color-stop(0.05, #005fbf), color-stop(1, #003f7f) );\
	background:-moz-linear-gradient( center top, #005fbf 5%%, #003f7f 100%% );\
	filter:progid:DXImageTransform.Microsoft.gradient(startColorstr=\"#005fbf\", endColorstr=\"#003f7f\");	background: -o-linear-gradient(top,#005fbf,003f7f);\
	background-color:#005fbf;\
}\
.Tablestyle tr:first-child td:first-child{\
	border-width:0px 0px 1px 0px;\
}\
.Tablestyle tr:first-child td:last-child{\
	border-width:0px 0px 1px 1px;\
}\
</style>\
<!--[if gte IE 9]>\
  <style type=\"text/css\">\
    .gradient {\
       filter: none;\
    }\
  </style>\
<![endif]-->\
</head>\
<body class=\"griadient\">");
        // </editor-fold>

        int i = 0;
        for (i = 2; i<argc; i++)
            Select(myconnection, argv[i]);

        printf("</body></html>");
        PQfinish(myconnection);
    }
    else
        printf("Za mało argumentow wywolania\nPrzyklad wywolania\n./nazwa_prog nazwa_pliku.csv\n");

    return (EXIT_SUCCESS);
}

