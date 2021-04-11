@ECHO ON

set PWD=%~dp0

SET PLANTUML="C:\Users\Denis BEURIVE\Documents\software\plantuml.jar"

python "%PWD%log2db.py" --log="%PWD%..\kad.txt" --db="%PWD%data\kad.db"
python "%PWD%log2plantuml.py" --db="%PWD%data\kad.db" > "%PWD%data\sequence.puml"
cd "%PWD%data"
java -jar %PLANTUML% sequence.puml
cd "%PWD%"

