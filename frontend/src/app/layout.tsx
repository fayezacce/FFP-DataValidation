import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Food Friendly Program Data Validator",
  description: "Validate and clean NID and DOB data from Excel sheets for the Food Friendly Program (FFP) — Directorate General of Food, Bangladesh",
  authors: [{ name: "Fayez Ahmed" }],
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}

