INSERT INTO TODOS (TEXT, COMPLETED) 
VALUES ($1, false) 
RETURNING *