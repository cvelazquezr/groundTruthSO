CREATE TABLE data (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT, 
    questionID INT NOT NULL, 
    answerID INT NOT NULL, 
    code TEXT NOT NULL, 
    imports TEXT NOT NULL, 
    PRIMARY KEY (id)
);