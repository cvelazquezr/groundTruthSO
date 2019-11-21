CREATE TABLE data (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT, 
    questionID INT NOT NULL, 
    answerID INT NOT NULL,  
    imports TEXT NOT NULL,
    code TEXT NOT NULL,
    PRIMARY KEY (id)
);