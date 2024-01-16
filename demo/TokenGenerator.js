const express = require('express');
const jwt = require("jsonwebtoken");
const router = express.Router();
const dotenv = require("dotenv");
const fs = require("fs");
const bodyParser = require('body-parser');

router.use(bodyParser.urlencoded({ extended: false }));
router.use(bodyParser.json());
dotenv.config();

router.post('/token', function (req, res) {
    const token = generateAccessToken(req.body);
    res.status(200).send({ auth: true, token: token });
});

function generateAccessToken(payload) {
    // expires after half and hour (1800 seconds = 30 minutes)

    const privateKey = fs.readFileSync('./private.key', 'utf8');

    return jwt.sign(payload, privateKey, { expiresIn: '3000s', algorithm: 'RS256', audience: 'http://localhost:3002' });
}

module.exports = router;