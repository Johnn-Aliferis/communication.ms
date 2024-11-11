# Use the official .NET SDK image to build the application
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /app

# Copy the .csproj file and restore dependencies
COPY *.csproj ./
RUN dotnet restore

# Copy the remaining application files and build the application
COPY . ./
RUN dotnet publish -c Release -o out

# Use a smaller runtime image to run the application
FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /app/out .

# Expose port 80
EXPOSE 80

# Set the entry point for the application
ENTRYPOINT ["dotnet", "communication.ms.API.dll"]