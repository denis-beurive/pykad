# Tools

Create a database from the LOG file:

    python log2db.py --log=<path to the LOG file> --db=<path to the database>

Generate PlantUML sequence diagram specification:

    python log2plantuml.py --db=<path to the database> > <file.puml>

Generate the graphical representation of the sequence diagram:

    SET PLANTUML="C:\Users\Denis BEURIVE\Documents\software\plantuml.jar"
    java -jar %PLANTUML% <file.puml>

