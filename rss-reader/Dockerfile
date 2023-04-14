# Use the official Node.js 16 image as the base image
FROM node

# Set the working directory inside the container
WORKDIR /app

# Copy package.json and package-lock.json files to the working directory
COPY package*.json ./

# Install the required npm packages
RUN npm install

# Copy the rest of the application code to the working directory
COPY . .

# Run the TypeScript application using ts-node
CMD ["npm", "start"]


# docker build -t rss-reader .
