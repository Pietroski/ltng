-- name: CreateUser :one
INSERT INTO users (user_id, email, name, surname, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING *;

-- name: GetUser :one
SELECT *
FROM users
WHERE user_id = $1
LIMIT 1;

-- name: GetUserByEmail :one
SELECT *
FROM users
WHERE email = $1
LIMIT 1;

-- name: DeleteUser :exec
DELETE
FROM users
WHERE user_id = $1;

-- name: DeleteUserByEmail :exec
DELETE
FROM users
WHERE email = $1;

-- name: UpdateUser :one
UPDATE users
SET (email, name, surname, updated_at) = ($1, $2, $3, $4)
WHERE user_id = $1
RETURNING *;

-- name: UpdateUserByEmail :one
UPDATE users
SET (user_id, email, name, surname, updated_at) = ($1, $2, $3, $4, $5)
WHERE email = $1
RETURNING *;

-- name: ListUsers :many
SELECT *
FROM users
ORDER BY row_id;

-- name: ListPaginatedUsers :many
SELECT *
FROM users
ORDER BY row_id
LIMIT $1 OFFSET $2;
