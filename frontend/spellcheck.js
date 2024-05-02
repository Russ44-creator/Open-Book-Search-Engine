const natural = require('natural');
const fs = require('fs');
const path = require('path');

// Load the dictionary file (this could be any large list of words)
const dictionaryPath = path.join(__dirname, 'dictionary.txt');
const spellcheck = new natural.Spellcheck(fs.readFileSync(dictionaryPath).toString().split('\n'));

// Function to check spelling and suggest corrections
function checkSpelling(word) {
  if (spellcheck.isCorrect(word)) {
    console.log(`The word "${word}" is spelled correctly.`);
  } else {
    const suggestions = spellcheck.getCorrections(word, 1); // 1 here represents the maximum distance for corrections
    if (suggestions.length > 0) {
      console.log(`Did you mean: ${suggestions.join(', ')}?`);
    } else {
      console.log(`No suggestions found for the word "${word}".`);
    }
  }
}

// Example usage
checkSpelling('welp');
module.exports = checkSpelling;

