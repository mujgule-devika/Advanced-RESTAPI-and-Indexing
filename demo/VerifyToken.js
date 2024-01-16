const jwt = require('jsonwebtoken');
const dotenv = require("dotenv");
const fs = require("fs");

dotenv.config();

function verifyToken(req, res, next) {
    const authHeader = req.headers['authorization']
    const token = authHeader && authHeader.split(' ')[1]

    if (token == null) return res.status(401).send({ auth: false, error: 'No token provided.' });
    const publicKey  = fs.readFileSync('./public.key', 'utf8');
    jwt.verify(token, publicKey , function(err, decodedToken) {
        if (err) return res.status(403).send({ auth: false, error: err });
        req.access = decodedToken;
        next();
    });
}

module.exports = verifyToken;
